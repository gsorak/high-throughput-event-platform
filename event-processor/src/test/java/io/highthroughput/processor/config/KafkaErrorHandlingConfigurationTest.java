package io.highthroughput.processor.config;

import io.highthroughput.contract.EnvelopeDecodingException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.util.backoff.BackOffExecution;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.stream.Stream;

import static io.highthroughput.processor.config.KafkaErrorHandlingConfiguration.failureClass;
import static io.highthroughput.processor.config.KafkaErrorHandlingConfiguration.isTransient;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Error classification and back-off of {@link KafkaErrorHandlingConfiguration}.
 * SQL errors are translated the way
 * {@code JdbcClient} translates them for PostgreSQL, from the SQLSTATE
 * PostgreSQL reports.
 */
class KafkaErrorHandlingConfigurationTest {

    private static final ProcessorProperties.Retry RETRY = new ProcessorProperties.Retry(Duration.ofMillis(500), 2.0,
            Duration.ofSeconds(30));

    private final SQLErrorCodeSQLExceptionTranslator translator = new SQLErrorCodeSQLExceptionTranslator("PostgreSQL");

    @Test
    void databaseUnavailableOrTimedOutIsTransient() {
        assertThat(isTransient(listenerFailure(sql("57014")))).as("statement timeout").isTrue();
        assertThat(isTransient(listenerFailure(sql("08006")))).as("connection lost").isTrue();
        assertThat(isTransient(listenerFailure(sql("57P01")))).as("server shutting down").isTrue();
        assertThat(isTransient(listenerFailure(sql("40P01")))).as("deadlock").isTrue();
        assertThat(isTransient(listenerFailure(new CannotCreateTransactionException("Could not open JDBC Connection",
                new SQLTransientConnectionException("Connection is not available, request timed out")))))
                .as("no pooled connection").isTrue();
    }

    @Test
    void failedRollbackAfterTransientFailureIsTransient() {
        // PostgreSQL stopped: the statement fails with 57P01, Hikari closes the
        // connection, the rollback fails without
        // an SQLSTATE and replaces the original exception.
        TransactionSystemException rollbackFailure = new TransactionSystemException("JDBC rollback failed",
                new SQLException("Connection is closed"));
        rollbackFailure.initApplicationException(sql("57P01"));

        assertThat(isTransient(listenerFailure(rollbackFailure))).isTrue();
        assertThat(failureClass(listenerFailure(rollbackFailure))).isEqualTo("transient");

        TransactionSystemException afterBug = new TransactionSystemException("JDBC rollback failed",
                new SQLException("Connection is closed"));
        afterBug.initApplicationException(new IllegalStateException("bug"));
        assertThat(isTransient(listenerFailure(afterBug))).isFalse();
    }

    @Test
    void failedRollbackAfterFailedCommitIsTransient() {
        // PostgreSQL stopped during the commit: the commit fails with 57P01, the
        // rollback fails without an SQLSTATE, and Spring only logs the commit failure.
        TransactionSystemException rollbackFailure = new TransactionSystemException("JDBC rollback failed",
                new SQLException("Connection is closed"));

        assertThat(isTransient(listenerFailure(rollbackFailure))).isTrue();
        assertThat(failureClass(listenerFailure(rollbackFailure))).isEqualTo("transient");
        assertThat(isTransient(listenerFailure(new TransactionSystemException("not from JDBC"))))
                .isFalse();
    }

    @Test
    void permanentAndUnclassifiedFailuresAreNotTransient() {
        assertThat(isTransient(listenerFailure(sql("22003")))).as("numeric overflow").isFalse();
        assertThat(isTransient(listenerFailure(new EnvelopeDecodingException("bad")))).as("malformed").isFalse();
        assertThat(isTransient(listenerFailure(new IllegalStateException("bug")))).as("unclassified").isFalse();
    }

    @Test
    void failuresAreCountedByClass() {
        assertThat(failureClass(listenerFailure(sql("08006")))).isEqualTo("transient");
        assertThat(failureClass(listenerFailure(sql("22003")))).isEqualTo("permanent");
        assertThat(failureClass(listenerFailure(new EnvelopeDecodingException("bad")))).isEqualTo("permanent");
        assertThat(failureClass(listenerFailure(new IllegalStateException("bug")))).isEqualTo("unclassified");
    }

    @Test
    void unclassifiedFailuresAreRetriedTenTimes() {
        BackOffExecution execution = KafkaErrorHandlingConfiguration.unclassifiedBackOff(RETRY).start();

        assertThat(Stream.generate(execution::nextBackOff).limit(11))
                .containsExactly(500L, 1000L, 2000L, 4000L, 8000L, 16000L, 30000L, 30000L, 30000L, 30000L,
                        BackOffExecution.STOP);
    }

    @Test
    void transientFailuresAreRetriedWithoutLimit() {
        BackOffExecution execution = KafkaErrorHandlingConfiguration.unlimitedBackOff(RETRY).start();

        assertThat(Stream.generate(execution::nextBackOff).limit(1000)).doesNotContain(BackOffExecution.STOP);
    }

    private DataAccessException sql(String sqlState) {
        return translator.translate("test", "INSERT ...", new SQLException("SQLSTATE " + sqlState, sqlState));
    }

    private static Exception listenerFailure(Throwable cause) {
        return new ListenerExecutionFailedException("Listener failed", cause);
    }
}
