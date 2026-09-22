package com.rrobetti.benchmark;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenLoopJdbcBenchmarkTest {

    @Test
    void normalReportVariantsReturnDistinctQueries() {
        String variant0 = OpenLoopJdbcBenchmark.normalReportSql(0);
        String variant1 = OpenLoopJdbcBenchmark.normalReportSql(1);
        String variant2 = OpenLoopJdbcBenchmark.normalReportSql(2);
        String variant99 = OpenLoopJdbcBenchmark.normalReportSql(99);

        assertAll(
                () -> assertTrue(variant0.contains("GROUP BY c.region")),
                () -> assertTrue(variant1.contains("GROUP BY p.category")),
                () -> assertTrue(variant2.contains("GROUP BY DATE(o.ordered_at)")),
                () -> assertEquals(variant2, variant99),
                () -> assertEquals(3, java.util.Set.of(variant0, variant1, variant2).size())
        );
    }

    @Test
    void expensiveReportVariantsReturnDistinctQueries() {
        String variant0 = OpenLoopJdbcBenchmark.expensiveReportSql(0);
        String variant1 = OpenLoopJdbcBenchmark.expensiveReportSql(1);
        String variant2 = OpenLoopJdbcBenchmark.expensiveReportSql(2);
        String variant99 = OpenLoopJdbcBenchmark.expensiveReportSql(99);

        assertAll(
                () -> assertTrue(variant0.contains("RANK() OVER")),
                () -> assertTrue(variant1.contains("co_purchase_count")),
                () -> assertTrue(variant2.contains("running_category_revenue")),
                () -> assertEquals(variant2, variant99),
                () -> assertEquals(3, java.util.Set.of(variant0, variant1, variant2).size())
        );
    }

    @Test
    void requestCountAllocationPreservesTotalAndRemainders() {
        EnumMap<OpenLoopJdbcBenchmark.RequestType, Integer> counts =
                OpenLoopJdbcBenchmark.calculateRequestCounts(7);

        assertAll(
                () -> assertEquals(7, counts.values().stream().mapToInt(Integer::intValue).sum()),
                () -> assertEquals(3, counts.get(OpenLoopJdbcBenchmark.RequestType.READ)),
                () -> assertEquals(1, counts.get(OpenLoopJdbcBenchmark.RequestType.CREATE)),
                () -> assertEquals(1, counts.get(OpenLoopJdbcBenchmark.RequestType.UPDATE)),
                () -> assertEquals(0, counts.get(OpenLoopJdbcBenchmark.RequestType.DELETE)),
                () -> assertEquals(2, counts.get(OpenLoopJdbcBenchmark.RequestType.NORMAL_REPORT)),
                () -> assertEquals(0, counts.get(OpenLoopJdbcBenchmark.RequestType.EXPENSIVE_REPORT))
        );
    }

    @Test
    void errorGroupingUsesOnlyExceptionType() {
        assertAll(
                () -> assertEquals("SQLException",
                        OpenLoopJdbcBenchmark.errorTypeKey(new SQLException("first message"))),
                () -> assertEquals("SQLException",
                        OpenLoopJdbcBenchmark.errorTypeKey(new SQLException("different message"))),
                () -> assertEquals("IllegalStateException",
                        OpenLoopJdbcBenchmark.errorTypeKey(new IllegalStateException("boom")))
        );
    }

    @Test
    void errorCountsAggregateByExceptionTypeNotMessage() {
        ConcurrentHashMap<String, LongAdder> errors = new ConcurrentHashMap<>();

        OpenLoopJdbcBenchmark.incrementErrorCount(errors, new SQLException("first message"));
        OpenLoopJdbcBenchmark.incrementErrorCount(errors, new SQLException("different message"));
        OpenLoopJdbcBenchmark.incrementErrorCount(errors, new IllegalStateException("boom"));

        assertAll(
                () -> assertEquals(2, errors.size()),
                () -> assertEquals(2L, errors.get("SQLException").sum()),
                () -> assertEquals(1L, errors.get("IllegalStateException").sum())
        );
    }

    @Test
    void ojpJdbcUrlUsesStandardPostgresTargetUrl() {
        assertEquals(
                "jdbc:ojp[localhost:1059]_postgresql://localhost:5432/benchmark",
                OpenLoopJdbcBenchmark.buildOjpJdbcUrl("localhost", 1059, "localhost", 5432, "benchmark")
        );
    }

    @Test
    void ojpPoolPropertiesRequestTwentyServerManagedConnections() throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("ojp.properties")) {
            assertTrue(inputStream != null, "ojp.properties should be packaged on the classpath");
            properties.load(inputStream);
        }

        assertAll(
                () -> assertEquals("20", properties.getProperty("ojp.connection.pool.maximumPoolSize")),
                () -> assertEquals("20", properties.getProperty("ojp.connection.pool.minimumIdle")),
                () -> assertEquals("10000", properties.getProperty("ojp.connection.pool.connectionTimeout"))
        );
    }

    @Test
    void createRequestsUseOneSqlStatement() {
        String sql = OpenLoopJdbcBenchmark.createRequestSql();

        assertAll(
                () -> assertTrue(sql.startsWith("WITH inserted_order AS")),
                () -> assertEquals(3, sql.split("INSERT INTO", -1).length - 1),
                () -> assertTrue(sql.contains("RETURNING id")),
                () -> assertTrue(sql.contains("SELECT 1")),
                () -> assertTrue(sql.contains("FROM inserted_event"))
        );
    }

    @Test
    void scheduledSubmissionTimeUsesConfiguredWaitMillis() {
        long baseNanos = 1_000_000_000L;

        assertAll(
                () -> assertEquals(baseNanos,
                        OpenLoopJdbcBenchmark.scheduledSubmissionTimeNanos(baseNanos, 5L, 0)),
                () -> assertEquals(baseNanos + 5_000_000L,
                        OpenLoopJdbcBenchmark.scheduledSubmissionTimeNanos(baseNanos, 5L, 1)),
                () -> assertEquals(baseNanos + 20_000_000L,
                        OpenLoopJdbcBenchmark.scheduledSubmissionTimeNanos(baseNanos, 5L, 4))
        );
    }

    @Test
    void validateDatasetStatsRejectsMissingSeedData() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> OpenLoopJdbcBenchmark.validateDatasetStats(
                        new OpenLoopJdbcBenchmark.DatasetStats(10, 10, 10, 10, 0),
                        "seed data missing"
                )
        );

        assertTrue(exception.getMessage().contains("seed data missing"));
    }
}
