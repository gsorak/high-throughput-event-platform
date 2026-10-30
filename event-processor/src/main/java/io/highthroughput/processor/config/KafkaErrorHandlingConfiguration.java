package io.highthroughput.processor.config;

import io.highthroughput.contract.EnvelopeDecodingException;
import io.highthroughput.processor.aggregation.DeadLetterRecoverer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * What happens when the listener throws. Spring Boot applies this
 * {@code CommonErrorHandler} bean to the
 * auto-configured listener container factory.
 *
 * <p>
 * {@link DefaultErrorHandler} seeks the consumer back to the failed record and
 * redelivers it after a back-off, so
 * the partition does not move past a record that could not be processed. Every
 * attempt runs the whole database
 * transaction again; a failed attempt left nothing behind. Three classes of
 * failure:
 * <ul>
 * <li><b>Permanent</b>: a malformed record ({@link EnvelopeDecodingException})
 * or an unprocessable one
 * ({@link DataIntegrityViolationException}, SQLSTATE classes 22 and 23 such as
 * a numeric overflow). Every attempt
 * would fail the same way, so the record goes to the dead-letter topic at
 * once.</li>
 * <li><b>Transient</b>: the database is unreachable, overloaded or timed out
 * ({@link #isTransient}). Retried with
 * exponential back-off and no limit: dead-lettering every record during an
 * outage would only move the outage into
 * the dead-letter topic. Lag grows instead and is visible.</li>
 * <li><b>Anything else</b> (unclassified, e.g. a bug): retried
 * {@value #UNCLASSIFIED_RETRIES} times with the same
 * back-off (about 2.5 minutes), then dead-lettered, so one bad record cannot
 * block its partition forever.</li>
 * </ul>
 * A record reaches the dead-letter topic or the aggregate, never neither: if
 * publishing to the dead-letter topic
 * fails, {@link DeadLetterRecoverer} throws, the offset is not committed and
 * the record is tried again.
 *
 * <p>
 * Every failed attempt increments
 * {@code processor.delivery.failures{class=transient|permanent|unclassified}}.
 * During a database outage this counter grows while nothing is aggregated.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandlingConfiguration {

    static final int UNCLASSIFIED_RETRIES = 10;

    // Spring's names for "a later attempt may succeed". A transaction that cannot
    // start (no connection) is reported
    // as CannotCreateTransactionException, outside the DataAccessException
    // hierarchy; the JDBC types cover causes
    // that were not translated.
    private static final List<Class<? extends Throwable>> TRANSIENT = List.of(
            TransientDataAccessException.class,
            DataAccessResourceFailureException.class,
            RecoverableDataAccessException.class,
            CannotCreateTransactionException.class,
            SQLTransientException.class,
            SQLRecoverableException.class);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(ProcessorProperties properties, KafkaOperations<?, ?> kafkaTemplate,
            MeterRegistry meterRegistry) {
        ProcessorProperties.Retry retry = properties.retry();
        BackOff unlimited = unlimitedBackOff(retry);

        DefaultErrorHandler handler = new DefaultErrorHandler(new DeadLetterRecoverer(kafkaTemplate, meterRegistry),
                unclassifiedBackOff(retry));
        handler.addNotRetryableExceptions(EnvelopeDecodingException.class, DataIntegrityViolationException.class);
        // null means "use the default back-off above". When the exception class changes
        // between attempts, the
        // handler starts over and asks again (resetStateOnExceptionChange, true by
        // default).
        handler.setBackOffFunction((record, exception) -> isTransient(exception) ? unlimited : null);

        Map<String, Counter> failures = new HashMap<>();
        for (String failureClass : List.of("transient", "permanent", "unclassified")) {
            failures.put(failureClass, Counter.builder("processor.delivery.failures")
                    .description("Failed attempts to process a record, by failure class")
                    .tag("class", failureClass)
                    .register(meterRegistry));
        }
        handler.setRetryListeners((record, exception, deliveryAttempt) -> failures
                .get(failureClass(exception)).increment());
        return handler;
    }

    static String failureClass(Throwable exception) {
        if (isTransient(exception)) {
            return "transient";
        }
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (t instanceof EnvelopeDecodingException || t instanceof DataIntegrityViolationException) {
                return "permanent";
            }
        }
        return "unclassified";
    }

    static ExponentialBackOff unlimitedBackOff(ProcessorProperties.Retry retry) {
        ExponentialBackOff backOff = new ExponentialBackOff(retry.initialInterval().toMillis(), retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        // Max elapsed time and max attempts keep their defaults: unlimited.
        return backOff;
    }

    static ExponentialBackOff unclassifiedBackOff(ProcessorProperties.Retry retry) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(UNCLASSIFIED_RETRIES);
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        return backOff;
    }

    /**
     * True if the exception or one of its causes means "the database is unavailable
     * right now".
     *
     * <p>
     * When PostgreSQL is stopped mid-transaction, the statement or the commit fails
     * with SQLSTATE 57P01, Hikari closes the connection, and the rollback then
     * fails
     * with Hikari's "Connection is closed" (no SQLSTATE). Spring throws that
     * rollback failure, a {@link TransactionSystemException}, instead of the
     * original
     * failure. If the statement failed, the original is kept as its application
     * exception and decides; if the commit failed, the original is only logged, and
     * a
     * commit or rollback that failed in JDBC counts as transient.
     */
    static boolean isTransient(Throwable exception) {
        for (Throwable t = exception; t != null; t = t.getCause()) {
            for (Class<? extends Throwable> type : TRANSIENT) {
                if (type.isInstance(t)) {
                    return true;
                }
            }
            if (t instanceof TransactionSystemException txFailure) {
                Throwable original = txFailure.getApplicationException();
                if (original != null) {
                    return isTransient(original);
                }
                if (txFailure.getCause() instanceof SQLException) {
                    return true;
                }
            }
        }
        return false;
    }
}
