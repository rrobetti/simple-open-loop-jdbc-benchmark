package com.rrobetti.benchmark;

import org.junit.jupiter.api.Test;

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
}
