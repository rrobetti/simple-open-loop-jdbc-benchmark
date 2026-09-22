package com.rrobetti.benchmark;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
