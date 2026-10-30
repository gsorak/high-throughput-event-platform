package io.highthroughput.ingest.ratelimit;

import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import io.github.bucket4j.redis.lettuce.Bucket4jLettuce;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.BucketProxy;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Token buckets stored in Valkey, shared by all API instances. Bucket4j updates
 * a bucket with compare-and-swap, so
 * concurrent requests from several instances never over-spend it.
 * <p>
 * The connection is opened before the API accepts requests (phase 0, like
 * {@code ProducerWarmUp}): on a fresh JVM the first connection and command took
 * ~1.6 s locally, and every request arriving meanwhile waited for it. If Valkey
 * is unreachable then, the API starts anyway and connects on first use.
 */
public class ValkeyBuckets implements SmartLifecycle, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ValkeyBuckets.class);

    /**
     * A bucket key is deleted once the bucket would be full again (plus this
     * margin), so idle tenants do not occupy
     * memory; a recreated bucket starts full, which is the same state.
     */
    private static final Duration KEY_EXPIRY_MARGIN = Duration.ofMinutes(1);

    private final RedisClient client;
    private volatile ProxyManager<String> proxyManager;
    private StatefulRedisConnection<String, byte[]> connection;
    private volatile boolean running;

    public ValkeyBuckets(String uri, Duration timeout) {
        client = RedisClient.create(uri);
        client.setOptions(ClientOptions.builder()
                // While disconnected, fail at once instead of queueing commands until they time
                // out.
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .socketOptions(SocketOptions.builder().connectTimeout(timeout).build())
                .timeoutOptions(TimeoutOptions.enabled(timeout))
                .build());
    }

    BucketProxy bucket(String tenantId, Supplier<BucketConfiguration> configuration) {
        return proxyManager().builder().build("rate-limit:" + tenantId, configuration);
    }

    /**
     * Connects on first use if {@link #start()} could not, so the API starts even
     * while Valkey is down.
     */
    private ProxyManager<String> proxyManager() {
        ProxyManager<String> manager = proxyManager;
        if (manager == null) {
            synchronized (this) {
                if (proxyManager == null) {
                    connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
                    proxyManager = Bucket4jLettuce.casBasedBuilder(connection)
                            .expirationAfterWrite(ExpirationAfterWriteStrategy
                                    .basedOnTimeForRefillingBucketUpToMax(KEY_EXPIRY_MARGIN))
                            .build();
                }
                manager = proxyManager;
            }
        }
        return manager;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    @Override
    public void start() {
        long startNanos = System.nanoTime();
        try {
            proxyManager();
            synchronized (this) {
                connection.sync().ping();
            }
            log.info("Valkey connected in {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        } catch (RuntimeException e) {
            log.warn("Valkey not reachable at startup; connecting on first use ({})", e.toString());
        }
        running = true;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public synchronized void close() {
        if (connection != null) {
            connection.close();
        }
        client.shutdown();
    }
}
