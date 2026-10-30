package com.gsorak.eventplatform.ratelimit;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

@Component
public class DistributedRateLimiter {

    private static final DefaultRedisScript<Long> TOKEN_BUCKET_SCRIPT = new DefaultRedisScript<>("""
            local key = KEYS[1]
            local capacity = tonumber(ARGV[1])
            local refill_rate = tonumber(ARGV[2])
            local ttl = tonumber(ARGV[3])
            local time = redis.call('TIME')
            local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            local values = redis.call('HMGET', key, 'tokens', 'updated_at')
            local tokens = tonumber(values[1]) or capacity
            local updated_at = tonumber(values[2]) or now
            tokens = math.min(capacity, tokens + math.max(0, now - updated_at) * refill_rate / 1000)
            if tokens < 1 then
                redis.call('HMSET', key, 'tokens', tokens, 'updated_at', now)
                redis.call('EXPIRE', key, ttl)
                return 0
            end
            redis.call('HMSET', key, 'tokens', tokens - 1, 'updated_at', now)
            redis.call('EXPIRE', key, ttl)
            return 1
            """, Long.class);

    private final StringRedisTemplate redisTemplate;
    private final int capacity;
    private final double refillPerSecond;
    private final int keyTtlSeconds;

    public DistributedRateLimiter(
            StringRedisTemplate redisTemplate,
            @Value("${app.rate-limit.capacity:100}") int capacity,
            @Value("${app.rate-limit.refill-per-second:50}") double refillPerSecond,
            @Value("${app.rate-limit.key-ttl-seconds:60}") int keyTtlSeconds
    ) {
        if (capacity < 1 || refillPerSecond <= 0 || keyTtlSeconds < 1) {
            throw new IllegalArgumentException("Rate limit settings must be positive");
        }
        this.redisTemplate = redisTemplate;
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.keyTtlSeconds = keyTtlSeconds;
    }

    public boolean tryAcquire(String clientId) {
        Long result = redisTemplate.execute(
                TOKEN_BUCKET_SCRIPT,
                List.of("event-platform:rate-limit:" + digest(clientId)),
                Integer.toString(capacity),
                Double.toString(refillPerSecond),
                Integer.toString(keyTtlSeconds)
        );
        return Long.valueOf(1).equals(result);
    }

    private String digest(String value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}
