package com.rrobetti.benchmark;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicLong;

public final class OpenLoopJdbcBenchmark {

    private static final String SCHEMA_NAME = "open_loop_benchmark";
    private static final long DATASET_RANDOM_SEED = 7_331L;
    private static final long WORKLOAD_RANDOM_SEED = 91_177L;
    private static final int HIKARI_POOL_SIZE = 100;

    // Request distribution (easy to change in one place).
    private static final int READ_PERCENT = 40;
    private static final int CREATE_PERCENT = 15;
    private static final int UPDATE_PERCENT = 15;
    private static final int DELETE_PERCENT = 5;
    private static final int NORMAL_REPORT_PERCENT = 20;
    private static final int EXPENSIVE_REPORT_PERCENT = 5;

    private static final List<String> CUSTOMER_STATUSES = List.of("ACTIVE", "INACTIVE", "VIP", "TRIAL");
    private static final List<String> CUSTOMER_TIERS = List.of("BRONZE", "SILVER", "GOLD", "PLATINUM");
    private static final List<String> REGIONS = List.of("NA", "EMEA", "LATAM", "APAC");
    private static final List<String> PRODUCT_CATEGORIES = List.of("BOOKS", "ELECTRONICS", "FASHION", "HOME", "TOYS", "SPORTS");
    private static final List<String> ORDER_STATUSES = List.of("NEW", "PROCESSING", "SHIPPED", "COMPLETED", "CANCELLED");
    private static final List<String> EVENT_TYPES = List.of("LOGIN", "SEARCH", "PAGE_VIEW", "CHECKOUT", "SUPPORT", "EMAIL_CLICK");

    private OpenLoopJdbcBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        if (Arrays.asList(args).contains("--help")) {
            printUsage();
            return;
        }

        validateWorkloadPercentages();
        BenchmarkConfig config = BenchmarkConfig.fromEnvironment();

        System.out.printf("Execution mode selected: %s%n", config.executionMode().displayName);
        System.out.printf("Configured request count: %d%n", config.requestCount());
        System.out.printf("Target PostgreSQL: %s:%d/%s%n", config.dbHost(), config.dbPort(), config.dbName());
        if (config.useOjp()) {
            System.out.printf("Open J Proxy endpoint: %s:%d%n", config.ojpHost(), config.ojpPort());
        }

        System.out.println("Database setup started.");
        long setupStart = System.nanoTime();
        DatasetStats datasetStats = setupDatabase(config);
        System.out.printf("Database setup finished in %s.%n", formatDuration(System.nanoTime() - setupStart));

        List<RequestPlan> requestPlans = buildRequestPlans(config, datasetStats);

        try (ConnectionProvider connectionProvider = config.useOjp()
                ? new OjpConnectionProvider(config)
                : new HikariConnectionProvider(config)) {
            System.out.println("Pool warm-up started.");
            long warmupStart = System.nanoTime();
            connectionProvider.warmUp();
            System.out.printf("Pool warm-up finished in %s.%n", formatDuration(System.nanoTime() - warmupStart));

            System.out.println("Benchmark started.");
            BenchmarkRun run = executeBenchmark(config, connectionProvider, requestPlans);
            System.out.printf("Benchmark finished in %s.%n", formatDuration(run.benchmarkDurationNanos()));
            printSummary(config, run);
        }
    }

    private static void printUsage() {
        System.out.println("""
                OpenLoopJdbcBenchmark

                Run with Maven:
                  mvn -Dexec.args=--help compile exec:java

                Important system properties:
                  export BENCHMARK_DB_PASSWORD=<db-password>
                  -Dbenchmark.useOjp=false|true
                  -Dbenchmark.requestCount=1000
                  -Dbenchmark.db.host=localhost
                  -Dbenchmark.db.port=5432
                  -Dbenchmark.db.name=benchmark
                  -Dbenchmark.db.user=postgres
                  -Dbenchmark.ojp.host=localhost
                  -Dbenchmark.ojp.port=1059
                  -Dbenchmark.dataset.customers=5000
                  -Dbenchmark.dataset.products=1000
                  -Dbenchmark.dataset.orders=25000
                  -Dbenchmark.dataset.events=50000
                """);
    }

    private static void validateWorkloadPercentages() {
        int total = READ_PERCENT + CREATE_PERCENT + UPDATE_PERCENT + DELETE_PERCENT
                + NORMAL_REPORT_PERCENT + EXPENSIVE_REPORT_PERCENT;
        if (total != 100) {
            throw new IllegalStateException("Workload percentages must total 100 but were " + total);
        }
    }

    private static DatasetStats setupDatabase(BenchmarkConfig config) throws SQLException {
        try (Connection connection = DriverManager.getConnection(config.directJdbcUrl(), config.dbUser(), config.dbPassword())) {
            connection.setAutoCommit(false);
            try {
                recreateSchema(connection);
                createTables(connection);
                createIndexes(connection);
                populateCustomers(connection, config);
                populateProducts(connection, config);
                long itemCount = populateOrdersAndItems(connection, config);
                populateEvents(connection, config);
                connection.commit();
                return new DatasetStats(config.orderCount(), itemCount, config.eventCount());
            } catch (SQLException exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    private static void recreateSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS " + SCHEMA_NAME + " CASCADE");
            statement.execute("CREATE SCHEMA " + SCHEMA_NAME);
        }
    }

    private static void createTables(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE %s.customers (
                        id BIGINT PRIMARY KEY,
                        customer_name VARCHAR(120) NOT NULL,
                        email VARCHAR(180) NOT NULL,
                        status VARCHAR(20) NOT NULL,
                        tier VARCHAR(20) NOT NULL,
                        region VARCHAR(20) NOT NULL,
                        created_at TIMESTAMP NOT NULL
                    )
                    """.formatted(SCHEMA_NAME));
            statement.execute("""
                    CREATE TABLE %s.products (
                        id BIGINT PRIMARY KEY,
                        sku VARCHAR(64) NOT NULL,
                        product_name VARCHAR(140) NOT NULL,
                        category VARCHAR(40) NOT NULL,
                        unit_price NUMERIC(12, 2) NOT NULL,
                        active BOOLEAN NOT NULL,
                        created_at TIMESTAMP NOT NULL
                    )
                    """.formatted(SCHEMA_NAME));
            statement.execute("""
                    CREATE TABLE %s.orders (
                        id BIGINT PRIMARY KEY,
                        customer_id BIGINT NOT NULL REFERENCES %s.customers(id),
                        order_status VARCHAR(20) NOT NULL,
                        ordered_at TIMESTAMP NOT NULL,
                        total_amount NUMERIC(14, 2) NOT NULL
                    )
                    """.formatted(SCHEMA_NAME, SCHEMA_NAME));
            statement.execute("""
                    CREATE TABLE %s.order_items (
                        id BIGINT PRIMARY KEY,
                        order_id BIGINT NOT NULL REFERENCES %s.orders(id),
                        product_id BIGINT NOT NULL REFERENCES %s.products(id),
                        quantity INTEGER NOT NULL,
                        unit_price NUMERIC(12, 2) NOT NULL,
                        line_total NUMERIC(14, 2) NOT NULL
                    )
                    """.formatted(SCHEMA_NAME, SCHEMA_NAME, SCHEMA_NAME));
            statement.execute("""
                    CREATE TABLE %s.activity_events (
                        id BIGINT PRIMARY KEY,
                        customer_id BIGINT NOT NULL REFERENCES %s.customers(id),
                        event_type VARCHAR(40) NOT NULL,
                        event_time TIMESTAMP NOT NULL,
                        details VARCHAR(200) NOT NULL
                    )
                    """.formatted(SCHEMA_NAME, SCHEMA_NAME));
        }
    }

    private static void createIndexes(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE INDEX idx_orders_customer ON " + SCHEMA_NAME + ".orders(customer_id)");
            statement.execute("CREATE INDEX idx_orders_ordered_at ON " + SCHEMA_NAME + ".orders(ordered_at)");
            statement.execute("CREATE INDEX idx_order_items_order ON " + SCHEMA_NAME + ".order_items(order_id)");
            statement.execute("CREATE INDEX idx_order_items_product ON " + SCHEMA_NAME + ".order_items(product_id)");
            statement.execute("CREATE INDEX idx_products_category ON " + SCHEMA_NAME + ".products(category)");
            statement.execute("CREATE INDEX idx_events_customer_time ON " + SCHEMA_NAME + ".activity_events(customer_id, event_time)");
        }
    }

    private static void populateCustomers(Connection connection, BenchmarkConfig config) throws SQLException {
        Random random = new Random(DATASET_RANDOM_SEED);
        String sql = """
                INSERT INTO %s.customers (id, customer_name, email, status, tier, region, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """.formatted(SCHEMA_NAME);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (long customerId = 1; customerId <= config.customerCount(); customerId++) {
                statement.setLong(1, customerId);
                statement.setString(2, "Customer " + customerId);
                statement.setString(3, "customer" + customerId + "@example.test");
                statement.setString(4, CUSTOMER_STATUSES.get(random.nextInt(CUSTOMER_STATUSES.size())));
                statement.setString(5, CUSTOMER_TIERS.get(random.nextInt(CUSTOMER_TIERS.size())));
                statement.setString(6, REGIONS.get(random.nextInt(REGIONS.size())));
                statement.setTimestamp(7, timestampDaysAgo(config.baseInstant(), random.nextInt(900)));
                statement.addBatch();
                flushBatchIfNeeded(statement, (int) customerId, 500);
            }
            statement.executeBatch();
        }
    }

    private static void populateProducts(Connection connection, BenchmarkConfig config) throws SQLException {
        Random random = new Random(DATASET_RANDOM_SEED * 3);
        String sql = """
                INSERT INTO %s.products (id, sku, product_name, category, unit_price, active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """.formatted(SCHEMA_NAME);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (long productId = 1; productId <= config.productCount(); productId++) {
                statement.setLong(1, productId);
                statement.setString(2, "SKU-%05d".formatted(productId));
                statement.setString(3, "Product " + productId);
                statement.setString(4, PRODUCT_CATEGORIES.get(random.nextInt(PRODUCT_CATEGORIES.size())));
                statement.setBigDecimal(5, priceForProduct(productId));
                statement.setBoolean(6, random.nextInt(100) >= 3);
                statement.setTimestamp(7, timestampDaysAgo(config.baseInstant(), random.nextInt(730)));
                statement.addBatch();
                flushBatchIfNeeded(statement, (int) productId, 500);
            }
            statement.executeBatch();
        }
    }

    private static long populateOrdersAndItems(Connection connection, BenchmarkConfig config) throws SQLException {
        Random random = new Random(DATASET_RANDOM_SEED * 5);
        String orderSql = """
                INSERT INTO %s.orders (id, customer_id, order_status, ordered_at, total_amount)
                VALUES (?, ?, ?, ?, ?)
                """.formatted(SCHEMA_NAME);
        String itemSql = """
                INSERT INTO %s.order_items (id, order_id, product_id, quantity, unit_price, line_total)
                VALUES (?, ?, ?, ?, ?, ?)
                """.formatted(SCHEMA_NAME);

        try (PreparedStatement orderStatement = connection.prepareStatement(orderSql);
             PreparedStatement itemStatement = connection.prepareStatement(itemSql)) {
            long itemId = 1;
            int batchedOrders = 0;
            for (long orderId = 1; orderId <= config.orderCount(); orderId++) {
                long customerId = 1 + random.nextInt(config.customerCount());
                Timestamp orderedAt = timestampDaysAgo(config.baseInstant(), random.nextInt(365));
                int itemCount = 1 + random.nextInt(5);
                BigDecimal orderTotal = BigDecimal.ZERO;

                for (int itemIndex = 0; itemIndex < itemCount; itemIndex++) {
                    long productId = 1 + random.nextInt(config.productCount());
                    int quantity = 1 + random.nextInt(4);
                    BigDecimal unitPrice = priceForProduct(productId);
                    BigDecimal lineTotal = unitPrice.multiply(BigDecimal.valueOf(quantity));
                    orderTotal = orderTotal.add(lineTotal);

                    itemStatement.setLong(1, itemId++);
                    itemStatement.setLong(2, orderId);
                    itemStatement.setLong(3, productId);
                    itemStatement.setInt(4, quantity);
                    itemStatement.setBigDecimal(5, unitPrice);
                    itemStatement.setBigDecimal(6, lineTotal);
                    itemStatement.addBatch();
                }

                orderStatement.setLong(1, orderId);
                orderStatement.setLong(2, customerId);
                orderStatement.setString(3, ORDER_STATUSES.get(random.nextInt(ORDER_STATUSES.size())));
                orderStatement.setTimestamp(4, orderedAt);
                orderStatement.setBigDecimal(5, scaleCurrency(orderTotal));
                orderStatement.addBatch();
                batchedOrders++;

                if (batchedOrders == 250) {
                    orderStatement.executeBatch();
                    itemStatement.executeBatch();
                    batchedOrders = 0;
                }
            }

            orderStatement.executeBatch();
            itemStatement.executeBatch();
            return itemId - 1;
        }
    }

    private static void populateEvents(Connection connection, BenchmarkConfig config) throws SQLException {
        Random random = new Random(DATASET_RANDOM_SEED * 7);
        String sql = """
                INSERT INTO %s.activity_events (id, customer_id, event_type, event_time, details)
                VALUES (?, ?, ?, ?, ?)
                """.formatted(SCHEMA_NAME);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (long eventId = 1; eventId <= config.eventCount(); eventId++) {
                long customerId = 1 + random.nextInt(config.customerCount());
                String eventType = EVENT_TYPES.get(random.nextInt(EVENT_TYPES.size()));
                statement.setLong(1, eventId);
                statement.setLong(2, customerId);
                statement.setString(3, eventType);
                statement.setTimestamp(4, timestampDaysAgo(config.baseInstant(), random.nextInt(180)));
                statement.setString(5, eventType + " event for customer " + customerId);
                statement.addBatch();
                flushBatchIfNeeded(statement, (int) eventId, 500);
            }
            statement.executeBatch();
        }
    }

    private static void flushBatchIfNeeded(PreparedStatement statement, int counter, int batchSize) throws SQLException {
        if (counter % batchSize == 0) {
            statement.executeBatch();
        }
    }

    private static List<RequestPlan> buildRequestPlans(BenchmarkConfig config, DatasetStats datasetStats) {
        EnumMap<RequestType, Integer> counts = calculateRequestCounts(config.requestCount());
        int deleteRequests = counts.getOrDefault(RequestType.DELETE, 0);
        if (deleteRequests > datasetStats.maxEventId()) {
            throw new IllegalArgumentException(
                    "Configured workload produces %d DELETE requests but only %d seeded events exist. "
                            .formatted(deleteRequests, datasetStats.maxEventId())
                            + "Increase benchmark.dataset.events or lower benchmark.requestCount."
            );
        }
        List<RequestPlan> plans = new ArrayList<>(config.requestCount());
        Random random = new Random(WORKLOAD_RANDOM_SEED);
        long nextCreateOrderId = datasetStats.maxOrderId() + 1L;
        long nextCreateItemId = datasetStats.maxOrderItemId() + 1L;
        long nextCreateEventId = datasetStats.maxEventId() + 1L;
        long nextDeleteEventId = 1L;

        for (RequestType type : RequestType.values()) {
            int count = counts.getOrDefault(type, 0);
            for (int index = 0; index < count; index++) {
                plans.add(new RequestPlan(
                        type,
                        1 + random.nextInt(config.customerCount()),
                        1 + random.nextInt(config.orderCount()),
                        1 + random.nextInt(config.productCount()),
                        nextDeleteEventId,
                        nextCreateOrderId,
                        nextCreateItemId,
                        nextCreateEventId,
                        1 + random.nextInt(4),
                        random.nextInt(3)
                ));

                if (type == RequestType.CREATE) {
                    nextCreateOrderId++;
                    nextCreateItemId++;
                    nextCreateEventId++;
                }
                if (type == RequestType.DELETE) {
                    nextDeleteEventId++;
                }
            }
        }

        Collections.shuffle(plans, new Random(WORKLOAD_RANDOM_SEED * 13));
        return plans;
    }

    static EnumMap<RequestType, Integer> calculateRequestCounts(int requestCount) {
        List<Allocation> allocations = new ArrayList<>();
        allocations.add(new Allocation(RequestType.READ, requestCount * READ_PERCENT / 100.0));
        allocations.add(new Allocation(RequestType.CREATE, requestCount * CREATE_PERCENT / 100.0));
        allocations.add(new Allocation(RequestType.UPDATE, requestCount * UPDATE_PERCENT / 100.0));
        allocations.add(new Allocation(RequestType.DELETE, requestCount * DELETE_PERCENT / 100.0));
        allocations.add(new Allocation(RequestType.NORMAL_REPORT, requestCount * NORMAL_REPORT_PERCENT / 100.0));
        allocations.add(new Allocation(RequestType.EXPENSIVE_REPORT, requestCount * EXPENSIVE_REPORT_PERCENT / 100.0));

        EnumMap<RequestType, Integer> result = new EnumMap<>(RequestType.class);
        int allocated = 0;
        for (Allocation allocation : allocations) {
            int floor = (int) Math.floor(allocation.exactCount());
            result.put(allocation.requestType(), floor);
            allocated += floor;
        }

        int remainder = requestCount - allocated;
        allocations.sort(Comparator.comparingDouble((Allocation allocation) -> allocation.exactCount() - Math.floor(allocation.exactCount())).reversed());
        for (int index = 0; index < remainder; index++) {
            Allocation allocation = allocations.get(index % allocations.size());
            result.merge(allocation.requestType(), 1, Integer::sum);
        }
        return result;
    }

    private static BenchmarkRun executeBenchmark(BenchmarkConfig config, ConnectionProvider connectionProvider, List<RequestPlan> plans) throws InterruptedException {
        RequestResult[] results = new RequestResult[plans.size()];
        LongAdder sqlStatementCount = new LongAdder();
        ConcurrentHashMap<String, LongAdder> errors = new ConcurrentHashMap<>();
        AtomicLong firstRequestStart = new AtomicLong(Long.MAX_VALUE);
        AtomicLong lastRequestFinish = new AtomicLong(Long.MIN_VALUE);

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(plans.size());

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int requestIndex = 0; requestIndex < plans.size(); requestIndex++) {
                final int arrayIndex = requestIndex;
                final RequestPlan plan = plans.get(requestIndex);
                executor.submit(() -> {
                    try {
                        startGate.await();
                        long startedAt = System.nanoTime();
                        updateMin(firstRequestStart, startedAt);
                        boolean success = false;
                        try {
                            executeRequest(config, connectionProvider, plan, sqlStatementCount);
                            success = true;
                        } catch (Exception exception) {
                            recordError(errors, exception);
                        } finally {
                            long finishedAt = System.nanoTime();
                            updateMax(lastRequestFinish, finishedAt);
                            results[arrayIndex] = new RequestResult(plan.type(), success, finishedAt - startedAt);
                        }
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        recordError(errors, interruptedException);
                        results[arrayIndex] = new RequestResult(plan.type(), false, 0L);
                    } finally {
                        doneGate.countDown();
                    }
                });
            }

            startGate.countDown();
            doneGate.await();
            long earliestStart = firstRequestStart.get();
            long latestFinish = lastRequestFinish.get();
            long benchmarkDuration = (earliestStart == Long.MAX_VALUE || latestFinish == Long.MIN_VALUE)
                    ? 0L
                    : Math.max(0L, latestFinish - earliestStart);
            return new BenchmarkRun(results, benchmarkDuration, sqlStatementCount.sum(), errors);
        }
    }

    private static void executeRequest(BenchmarkConfig config, ConnectionProvider connectionProvider, RequestPlan plan, LongAdder sqlStatementCount) throws SQLException {
        switch (plan.type()) {
            case READ -> executeReadRequest(connectionProvider, plan, sqlStatementCount);
            case CREATE -> executeCreateRequest(config, connectionProvider, plan, sqlStatementCount);
            case UPDATE -> executeUpdateRequest(connectionProvider, plan, sqlStatementCount);
            case DELETE -> executeDeleteRequest(connectionProvider, plan, sqlStatementCount);
            case NORMAL_REPORT -> executeNormalReportRequest(config, connectionProvider, plan, sqlStatementCount);
            case EXPENSIVE_REPORT -> executeExpensiveReportRequest(config, connectionProvider, plan, sqlStatementCount);
        }
    }

    private static void executeReadRequest(ConnectionProvider connectionProvider, RequestPlan plan, LongAdder sqlStatementCount) throws SQLException {
        String sql = """
                SELECT c.id, c.customer_name, c.region, o.id, o.order_status, o.total_amount, o.ordered_at
                FROM %s.customers c
                LEFT JOIN %s.orders o ON o.customer_id = c.id
                WHERE c.id = ?
                ORDER BY o.ordered_at DESC NULLS LAST
                LIMIT 10
                """.formatted(SCHEMA_NAME, SCHEMA_NAME);
        try (Connection connection = connectionProvider.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, plan.customerId());
            consumeQuery(statement, sqlStatementCount);
        }
    }

    private static void executeCreateRequest(BenchmarkConfig config, ConnectionProvider connectionProvider, RequestPlan plan, LongAdder sqlStatementCount) throws SQLException {
        String orderSql = """
                INSERT INTO %s.orders (id, customer_id, order_status, ordered_at, total_amount)
                VALUES (?, ?, ?, ?, ?)
                """.formatted(SCHEMA_NAME);
        String itemSql = """
                INSERT INTO %s.order_items (id, order_id, product_id, quantity, unit_price, line_total)
                VALUES (?, ?, ?, ?, ?, ?)
                """.formatted(SCHEMA_NAME);
        String eventSql = """
                INSERT INTO %s.activity_events (id, customer_id, event_type, event_time, details)
                VALUES (?, ?, ?, ?, ?)
                """.formatted(SCHEMA_NAME);

        BigDecimal unitPrice = priceForProduct(plan.productId());
        BigDecimal lineTotal = scaleCurrency(unitPrice.multiply(BigDecimal.valueOf(plan.quantity())));
        Timestamp now = Timestamp.from(Instant.now());

        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement orderStatement = connection.prepareStatement(orderSql);
                 PreparedStatement itemStatement = connection.prepareStatement(itemSql);
                 PreparedStatement eventStatement = connection.prepareStatement(eventSql)) {

                orderStatement.setLong(1, plan.createOrderId());
                orderStatement.setLong(2, plan.customerId());
                orderStatement.setString(3, "NEW");
                orderStatement.setTimestamp(4, now);
                orderStatement.setBigDecimal(5, lineTotal);
                executeUpdate(orderStatement, sqlStatementCount);

                itemStatement.setLong(1, plan.createItemId());
                itemStatement.setLong(2, plan.createOrderId());
                itemStatement.setLong(3, plan.productId());
                itemStatement.setInt(4, plan.quantity());
                itemStatement.setBigDecimal(5, unitPrice);
                itemStatement.setBigDecimal(6, lineTotal);
                executeUpdate(itemStatement, sqlStatementCount);

                eventStatement.setLong(1, plan.createEventId());
                eventStatement.setLong(2, plan.customerId());
                eventStatement.setString(3, "CHECKOUT");
                eventStatement.setTimestamp(4, now);
                eventStatement.setString(5, "Synthetic order " + plan.createOrderId());
                executeUpdate(eventStatement, sqlStatementCount);

                connection.commit();
            } catch (SQLException exception) {
                connection.rollback();
                throw exception;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    private static void executeUpdateRequest(ConnectionProvider connectionProvider, RequestPlan plan, LongAdder sqlStatementCount) throws SQLException {
        String sql = """
                UPDATE %s.orders
                SET order_status = ?, total_amount = total_amount + ?
                WHERE id = ?
                """.formatted(SCHEMA_NAME);
        try (Connection connection = connectionProvider.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, "PROCESSING");
            statement.setBigDecimal(2, BigDecimal.valueOf((plan.quantity() * 25L), 2));
            statement.setLong(3, plan.orderId());
            executeUpdate(statement, sqlStatementCount);
        }
    }

    private static void executeDeleteRequest(ConnectionProvider connectionProvider, RequestPlan plan, LongAdder sqlStatementCount) throws SQLException {
        String sql = "DELETE FROM " + SCHEMA_NAME + ".activity_events WHERE id = ?";
        try (Connection connection = connectionProvider.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, plan.deleteEventId());
            int rowsDeleted = executeUpdate(statement, sqlStatementCount);
            if (rowsDeleted == 0) {
                throw new SQLException("Delete request removed no rows for event " + plan.deleteEventId());
            }
        }
    }

    private static void executeNormalReportRequest(BenchmarkConfig config, ConnectionProvider connectionProvider, RequestPlan plan, LongAdder sqlStatementCount) throws SQLException {
        String sql = normalReportSql(plan.reportVariant());

        try (Connection connection = connectionProvider.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, timestampDaysAgo(config.baseInstant(), 90));
            consumeQuery(statement, sqlStatementCount);
        }
    }

    private static void executeExpensiveReportRequest(BenchmarkConfig config, ConnectionProvider connectionProvider, RequestPlan plan, LongAdder sqlStatementCount) throws SQLException {
        String sql = expensiveReportSql(plan.reportVariant());

        try (Connection connection = connectionProvider.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, timestampDaysAgo(config.baseInstant(), 365));
            consumeQuery(statement, sqlStatementCount);
        }
    }

    static String normalReportSql(int reportVariant) {
        return switch (reportVariant) {
            case 0 -> """
                    SELECT c.region,
                           COUNT(DISTINCT o.id) AS order_count,
                           COUNT(DISTINCT c.id) AS customer_count,
                           COALESCE(SUM(o.total_amount), 0) AS revenue
                    FROM %s.orders o
                    JOIN %s.customers c ON c.id = o.customer_id
                    WHERE o.ordered_at >= ?
                    GROUP BY c.region
                    ORDER BY revenue DESC
                    """.formatted(SCHEMA_NAME, SCHEMA_NAME);
            case 1 -> """
                    SELECT p.category,
                           COUNT(*) AS line_count,
                           SUM(oi.quantity) AS units,
                           SUM(oi.line_total) AS revenue
                    FROM %s.order_items oi
                    JOIN %s.products p ON p.id = oi.product_id
                    JOIN %s.orders o ON o.id = oi.order_id
                    WHERE o.ordered_at >= ?
                    GROUP BY p.category
                    ORDER BY revenue DESC
                    """.formatted(SCHEMA_NAME, SCHEMA_NAME, SCHEMA_NAME);
            default -> """
                    SELECT DATE(o.ordered_at) AS order_day,
                           COUNT(*) AS orders,
                           COUNT(DISTINCT o.customer_id) AS active_customers,
                           SUM(o.total_amount) AS revenue
                    FROM %s.orders o
                    WHERE o.ordered_at >= ?
                    GROUP BY DATE(o.ordered_at)
                    ORDER BY order_day DESC
                    """.formatted(SCHEMA_NAME);
        };
    }

    static String expensiveReportSql(int reportVariant) {
        return switch (reportVariant) {
            // EXPENSIVE QUERY #1
            case 0 -> """
                    SELECT ranked.customer_id,
                           ranked.customer_name,
                           ranked.total_spend,
                           ranked.spend_rank
                    FROM (
                        SELECT c.id AS customer_id,
                               c.customer_name,
                               SUM(oi.line_total) AS total_spend,
                               RANK() OVER (ORDER BY SUM(oi.line_total) DESC) AS spend_rank
                        FROM %s.customers c
                        JOIN %s.orders o ON o.customer_id = c.id
                        JOIN %s.order_items oi ON oi.order_id = o.id
                        WHERE o.ordered_at >= ?
                        GROUP BY c.id, c.customer_name
                    ) ranked
                    WHERE ranked.spend_rank <= 50
                    ORDER BY ranked.spend_rank, ranked.customer_id
                    """.formatted(SCHEMA_NAME, SCHEMA_NAME, SCHEMA_NAME);
            // EXPENSIVE QUERY #2
            case 1 -> """
                    SELECT p.category,
                           oi1.product_id AS product_a,
                           oi2.product_id AS product_b,
                           COUNT(*) AS co_purchase_count
                    FROM %s.order_items oi1
                    JOIN %s.order_items oi2
                      ON oi1.order_id = oi2.order_id
                     AND oi1.product_id < oi2.product_id
                    JOIN %s.orders o ON o.id = oi1.order_id
                    JOIN %s.products p ON p.id = oi1.product_id
                    WHERE o.ordered_at >= ?
                    GROUP BY p.category, oi1.product_id, oi2.product_id
                    ORDER BY co_purchase_count DESC, product_a, product_b
                    LIMIT 100
                    """.formatted(SCHEMA_NAME, SCHEMA_NAME, SCHEMA_NAME, SCHEMA_NAME);
            // EXPENSIVE QUERY #3
            default -> """
                    SELECT revenue.category,
                           revenue.order_month,
                           revenue.monthly_revenue,
                           revenue.running_category_revenue,
                           revenue.month_row_number
                    FROM (
                        SELECT p.category,
                               DATE_TRUNC('month', o.ordered_at) AS order_month,
                               SUM(oi.line_total) AS monthly_revenue,
                               SUM(SUM(oi.line_total)) OVER (
                                   PARTITION BY p.category
                                   ORDER BY DATE_TRUNC('month', o.ordered_at)
                               ) AS running_category_revenue,
                               ROW_NUMBER() OVER (
                                   PARTITION BY p.category
                                   ORDER BY DATE_TRUNC('month', o.ordered_at)
                               ) AS month_row_number
                        FROM %s.orders o
                        JOIN %s.order_items oi ON oi.order_id = o.id
                        JOIN %s.products p ON p.id = oi.product_id
                        WHERE o.ordered_at >= ?
                        GROUP BY p.category, DATE_TRUNC('month', o.ordered_at)
                    ) revenue
                    ORDER BY revenue.running_category_revenue DESC, revenue.category
                    LIMIT 200
                    """.formatted(SCHEMA_NAME, SCHEMA_NAME, SCHEMA_NAME);
        };
    }

    private static void consumeQuery(PreparedStatement statement, LongAdder sqlStatementCount) throws SQLException {
        sqlStatementCount.increment();
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                resultSet.getObject(1);
            }
        }
    }

    private static int executeUpdate(PreparedStatement statement, LongAdder sqlStatementCount) throws SQLException {
        sqlStatementCount.increment();
        return statement.executeUpdate();
    }

    private static void recordError(ConcurrentHashMap<String, LongAdder> errors, Exception exception) {
        errors.computeIfAbsent(errorTypeKey(exception), ignored -> new LongAdder()).increment();
    }

    static String errorTypeKey(Exception exception) {
        return exception.getClass().getSimpleName();
    }

    private static void printSummary(BenchmarkConfig config, BenchmarkRun run) {
        Summary summary = Summary.from(run);

        System.out.println();
        System.out.println("========== BENCHMARK SUMMARY ==========");
        System.out.printf("execution mode: %s%n", config.executionMode().displayName);
        System.out.printf("configured request count: %d%n", config.requestCount());
        System.out.printf("total requests attempted: %d%n", summary.totalAttempted());
        System.out.printf("total SQL statements actually executed: %d%n", run.sqlStatementCount());
        System.out.printf("total successful requests: %d%n", summary.successCount());
        System.out.printf("success percentage: %s%n", formatPercent(summary.successRate()));
        System.out.printf("total failed requests: %d%n", summary.failureCount());
        System.out.printf("failure percentage: %s%n", formatPercent(summary.failureRate()));
        System.out.printf("total benchmark duration: %s%n", formatDuration(run.benchmarkDurationNanos()));
        System.out.printf("overall requests/second: %.2f%n", requestsPerSecond(summary.totalAttempted(), run.benchmarkDurationNanos()));
        System.out.printf("successful requests/second: %.2f%n", requestsPerSecond(summary.successCount(), run.benchmarkDurationNanos()));

        printLatencySection("Successful request latency", summary.successLatency());
        printLatencySection("Failed request latency", summary.failureLatency());
        printLatencySection("Overall request latency", summary.overallLatency());

        System.out.println();
        System.out.println("Per request type breakdown:");
        for (RequestType type : RequestType.values()) {
            TypeSummary typeSummary = summary.typeSummaries().get(type);
            System.out.printf(
                    Locale.US,
                    "  %-17s attempted=%-5d successes=%-5d failures=%-5d success=%-8s avg=%-10s p95=%s%n",
                    type.displayName,
                    typeSummary.attempted(),
                    typeSummary.successes(),
                    typeSummary.failures(),
                    formatPercent(typeSummary.successRate()),
                    formatMillis(typeSummary.averageLatencyNanos()),
                    formatMillis(typeSummary.p95LatencyNanos())
            );
        }

        if (!run.errors().isEmpty()) {
            System.out.println();
            System.out.println("Exception counts:");
            run.errors().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> System.out.printf("  %s -> %d%n", entry.getKey(), entry.getValue().sum()));
        }
        System.out.println("=======================================");
    }

    private static void printLatencySection(String title, LatencySummary latencySummary) {
        System.out.println();
        System.out.println(title + ":");
        System.out.printf("  average: %s%n", formatMillis(latencySummary.averageNanos()));
        System.out.printf("  minimum: %s%n", formatMillis(latencySummary.minNanos()));
        System.out.printf("  maximum: %s%n", formatMillis(latencySummary.maxNanos()));
        System.out.printf("  p50: %s%n", formatMillis(latencySummary.p50Nanos()));
        System.out.printf("  p95: %s%n", formatMillis(latencySummary.p95Nanos()));
        System.out.printf("  p99: %s%n", formatMillis(latencySummary.p99Nanos()));
    }

    private static double requestsPerSecond(long completedRequests, long durationNanos) {
        if (durationNanos == 0L) {
            return 0.0;
        }
        return completedRequests * 1_000_000_000.0 / durationNanos;
    }

    private static String formatPercent(double percentage) {
        return String.format(Locale.US, "%.2f%%", percentage);
    }

    private static String formatDuration(long nanos) {
        return Duration.ofNanos(Math.max(nanos, 0L)).toString();
    }

    private static String formatMillis(long nanos) {
        if (nanos < 0) {
            return "n/a";
        }
        return String.format(Locale.US, "%.3f ms", nanos / 1_000_000.0);
    }

    private static Timestamp timestampDaysAgo(Instant baseInstant, int daysAgo) {
        return Timestamp.from(baseInstant.minus(Duration.ofDays(daysAgo)));
    }

    private static BigDecimal priceForProduct(long productId) {
        long cents = 500 + ((productId * 137) % 25_000);
        return BigDecimal.valueOf(cents, 2);
    }

    private static BigDecimal scaleCurrency(BigDecimal value) {
        return value.setScale(2, RoundingMode.HALF_UP);
    }

    private static void updateMin(AtomicLong target, long value) {
        long current = target.get();
        while (value < current && !target.compareAndSet(current, value)) {
            current = target.get();
        }
    }

    private static void updateMax(AtomicLong target, long value) {
        long current = target.get();
        while (value > current && !target.compareAndSet(current, value)) {
            current = target.get();
        }
    }

    private interface ConnectionProvider extends AutoCloseable {
        Connection getConnection() throws SQLException;

        void warmUp() throws SQLException;

        @Override
        void close() throws SQLException;
    }

    private static final class HikariConnectionProvider implements ConnectionProvider {
        private final HikariDataSource dataSource;

        private HikariConnectionProvider(BenchmarkConfig config) {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(config.directJdbcUrl());
            hikariConfig.setUsername(config.dbUser());
            hikariConfig.setPassword(config.dbPassword());
            hikariConfig.setMinimumIdle(HIKARI_POOL_SIZE);
            hikariConfig.setMaximumPoolSize(HIKARI_POOL_SIZE);
            hikariConfig.setInitializationFailTimeout(-1);
            hikariConfig.setConnectionTimeout(30_000);
            hikariConfig.setPoolName("benchmark-hikari");
            this.dataSource = new HikariDataSource(hikariConfig);
        }

        @Override
        public Connection getConnection() throws SQLException {
            return dataSource.getConnection();
        }

        @Override
        public void warmUp() throws SQLException {
            List<Connection> heldConnections = new ArrayList<>(HIKARI_POOL_SIZE);
            try {
                for (int index = 0; index < HIKARI_POOL_SIZE; index++) {
                    Connection connection = dataSource.getConnection();
                    heldConnections.add(connection);
                    try (Statement statement = connection.createStatement()) {
                        statement.execute("SELECT 1");
                    }
                }
            } finally {
                for (Connection connection : heldConnections) {
                    connection.close();
                }
            }
        }

        @Override
        public void close() {
            dataSource.close();
        }
    }

    private static final class OjpConnectionProvider implements ConnectionProvider {
        private final BenchmarkConfig config;

        private OjpConnectionProvider(BenchmarkConfig config) {
            this.config = Objects.requireNonNull(config);
            try {
                Class.forName("org.openjproxy.jdbc.Driver");
            } catch (ClassNotFoundException exception) {
                throw new IllegalStateException("OJP driver not available on the classpath.", exception);
            }
        }

        @Override
        public Connection getConnection() throws SQLException {
            Properties properties = new Properties();
            properties.setProperty("user", config.dbUser());
            properties.setProperty("password", config.dbPassword());
            return DriverManager.getConnection(config.ojpJdbcUrl(), properties);
        }

        @Override
        public void warmUp() throws SQLException {
            try (Connection connection = getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1");
            }
        }

        @Override
        public void close() {
            // No pooled state to close.
        }
    }

    private record BenchmarkConfig(
            boolean useOjp,
            int requestCount,
            String dbHost,
            int dbPort,
            String dbName,
            String dbUser,
            String dbPassword,
            String ojpHost,
            int ojpPort,
            int customerCount,
            int productCount,
            int orderCount,
            int eventCount,
            Instant baseInstant
    ) {
        private static BenchmarkConfig fromEnvironment() {
            boolean useOjp = boolProperty("benchmark.useOjp", false);
            int requestCount = intProperty("benchmark.requestCount", 1_000);
            int customerCount = intProperty("benchmark.dataset.customers", 5_000);
            int productCount = intProperty("benchmark.dataset.products", 1_000);
            int orderCount = intProperty("benchmark.dataset.orders", 25_000);
            int eventCount = intProperty("benchmark.dataset.events", 50_000);
            BenchmarkConfig config = new BenchmarkConfig(
                    useOjp,
                    requestCount,
                    stringProperty("benchmark.db.host", "BENCHMARK_DB_HOST", "localhost"),
                    intProperty("benchmark.db.port", env("BENCHMARK_DB_PORT", "5432")),
                    stringProperty("benchmark.db.name", "BENCHMARK_DB_NAME", "benchmark"),
                    stringProperty("benchmark.db.user", "BENCHMARK_DB_USER", "postgres"),
                    stringProperty("benchmark.db.password", "BENCHMARK_DB_PASSWORD", "postgres"),
                    stringProperty("benchmark.ojp.host", "BENCHMARK_OJP_HOST", "localhost"),
                    intProperty("benchmark.ojp.port", env("BENCHMARK_OJP_PORT", "1059")),
                    customerCount,
                    productCount,
                    orderCount,
                    eventCount,
                    Instant.now()
            );

            if (config.requestCount() <= 0 || config.customerCount() <= 0 || config.productCount() <= 0
                    || config.orderCount() <= 0 || config.eventCount() <= 0) {
                throw new IllegalArgumentException("Request and dataset sizes must be positive.");
            }
            return config;
        }

        private ExecutionMode executionMode() {
            return useOjp ? ExecutionMode.OJP : ExecutionMode.HIKARI;
        }

        private String directJdbcUrl() {
            return "jdbc:postgresql://%s:%d/%s".formatted(dbHost, dbPort, dbName);
        }

        private String ojpJdbcUrl() {
            return "jdbc:ojp[%s:%d]_postgresql://%s@%s:%d/%s".formatted(
                    ojpHost,
                    ojpPort,
                    dbUser,
                    dbHost,
                    dbPort,
                    dbName
            );
        }

        private static boolean boolProperty(String systemProperty, boolean defaultValue) {
            return Boolean.parseBoolean(System.getProperty(systemProperty, env(propertyToEnv(systemProperty), Boolean.toString(defaultValue))));
        }

        private static int intProperty(String property, int defaultValue) {
            return Integer.parseInt(System.getProperty(property, Integer.toString(defaultValue)));
        }

        private static int intProperty(String property, String defaultValue) {
            return Integer.parseInt(System.getProperty(property, defaultValue));
        }

        private static String stringProperty(String systemProperty, String envVariable, String defaultValue) {
            return System.getProperty(systemProperty, env(envVariable, defaultValue));
        }

        private static String propertyToEnv(String property) {
            return property.toUpperCase(Locale.ROOT).replace('.', '_');
        }

        private static String env(String envVariable, String defaultValue) {
            String value = System.getenv(envVariable);
            return value == null || value.isBlank() ? defaultValue : value;
        }
    }

    private enum ExecutionMode {
        HIKARI("HIKARI"),
        OJP("OJP");

        private final String displayName;

        ExecutionMode(String displayName) {
            this.displayName = displayName;
        }
    }

    enum RequestType {
        READ("READ"),
        CREATE("CREATE"),
        UPDATE("UPDATE"),
        DELETE("DELETE"),
        NORMAL_REPORT("REPORT"),
        EXPENSIVE_REPORT("EXPENSIVE_REPORT");

        private final String displayName;

        RequestType(String displayName) {
            this.displayName = displayName;
        }
    }

    private record Allocation(RequestType requestType, double exactCount) {
    }

    private record DatasetStats(long maxOrderId, long maxOrderItemId, long maxEventId) {
    }

    private record RequestPlan(
            RequestType type,
            long customerId,
            long orderId,
            long productId,
            long deleteEventId,
            long createOrderId,
            long createItemId,
            long createEventId,
            int quantity,
            int reportVariant
    ) {
    }

    private record RequestResult(RequestType type, boolean success, long latencyNanos) {
    }

    private record BenchmarkRun(
            RequestResult[] results,
            long benchmarkDurationNanos,
            long sqlStatementCount,
            ConcurrentHashMap<String, LongAdder> errors
    ) {
    }

    private record Summary(
            long totalAttempted,
            long successCount,
            long failureCount,
            LatencySummary successLatency,
            LatencySummary failureLatency,
            LatencySummary overallLatency,
            EnumMap<RequestType, TypeSummary> typeSummaries
    ) {
        private static Summary from(BenchmarkRun run) {
            List<Long> successLatencies = new ArrayList<>();
            List<Long> failureLatencies = new ArrayList<>();
            List<Long> overallLatencies = new ArrayList<>();
            EnumMap<RequestType, List<RequestResult>> byType = new EnumMap<>(RequestType.class);

            for (RequestType requestType : RequestType.values()) {
                byType.put(requestType, new ArrayList<>());
            }

            for (RequestResult result : run.results()) {
                if (result == null) {
                    continue;
                }
                overallLatencies.add(result.latencyNanos());
                byType.get(result.type()).add(result);
                if (result.success()) {
                    successLatencies.add(result.latencyNanos());
                } else {
                    failureLatencies.add(result.latencyNanos());
                }
            }

            EnumMap<RequestType, TypeSummary> typeSummaries = new EnumMap<>(RequestType.class);
            for (RequestType requestType : RequestType.values()) {
                List<RequestResult> results = byType.get(requestType);
                long successes = results.stream().filter(RequestResult::success).count();
                long failures = results.size() - successes;
                long[] latencies = results.stream().mapToLong(RequestResult::latencyNanos).toArray();
                typeSummaries.put(requestType, new TypeSummary(
                        results.size(),
                        successes,
                        failures,
                        average(latencies),
                        percentile(latencies, 95)
                ));
            }

            return new Summary(
                    run.results().length,
                    successLatencies.size(),
                    failureLatencies.size(),
                    LatencySummary.from(successLatencies),
                    LatencySummary.from(failureLatencies),
                    LatencySummary.from(overallLatencies),
                    typeSummaries
            );
        }

        private double successRate() {
            return percentage(successCount, totalAttempted);
        }

        private double failureRate() {
            return percentage(failureCount, totalAttempted);
        }
    }

    private record TypeSummary(
            long attempted,
            long successes,
            long failures,
            long averageLatencyNanos,
            long p95LatencyNanos
    ) {
        private double successRate() {
            return percentage(successes, attempted);
        }
    }

    private record LatencySummary(
            long averageNanos,
            long minNanos,
            long maxNanos,
            long p50Nanos,
            long p95Nanos,
            long p99Nanos
    ) {
        private static LatencySummary from(List<Long> latencies) {
            long[] values = latencies.stream().mapToLong(Long::longValue).toArray();
            return new LatencySummary(
                    average(values),
                    values.length == 0 ? -1L : Arrays.stream(values).min().orElse(-1L),
                    values.length == 0 ? -1L : Arrays.stream(values).max().orElse(-1L),
                    percentile(values, 50),
                    percentile(values, 95),
                    percentile(values, 99)
            );
        }
    }

    private static long average(long[] values) {
        if (values.length == 0) {
            return -1L;
        }
        double total = 0.0;
        for (long value : values) {
            total += value;
        }
        return Math.round(total / values.length);
    }

    private static long percentile(long[] values, int percentile) {
        if (values.length == 0) {
            return -1L;
        }
        long[] copy = Arrays.copyOf(values, values.length);
        Arrays.sort(copy);
        int index = (int) Math.ceil((percentile / 100.0) * copy.length) - 1;
        index = Math.max(0, Math.min(index, copy.length - 1));
        return copy[index];
    }

    private static double percentage(long numerator, long denominator) {
        if (denominator == 0L) {
            return 0.0;
        }
        return numerator * 100.0 / denominator;
    }
}
