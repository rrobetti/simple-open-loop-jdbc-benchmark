# simple-open-loop-jdbc-benchmark

Self-contained Java 21 benchmark for comparing direct PostgreSQL access through HikariCP against Open J Proxy 1.0.0 under an open-loop burst.

## Maven dependencies

```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.7</version>
</dependency>
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>6.3.0</version>
</dependency>
<dependency>
    <groupId>org.openjproxy</groupId>
    <artifactId>ojp-jdbc-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Build

```bash
mvn clean package
```

## Run Hikari mode

This mode connects directly to PostgreSQL and warms a 100-connection Hikari pool before timing starts.

```bash
export BENCHMARK_DB_PASSWORD=<db-password>
mvn \
  -Dbenchmark.useOjp=false \
  -Dbenchmark.requestCount=1000 \
  -Dbenchmark.db.host=localhost \
  -Dbenchmark.db.port=5432 \
  -Dbenchmark.db.name=benchmark \
  -Dbenchmark.db.user=postgres \
  exec:java
```

## Run OJP mode

This mode uses the Open J Proxy JDBC driver directly with no client-side pool. It assumes the OJP server is available on `localhost:1059`.

```bash
export BENCHMARK_DB_PASSWORD=<db-password>
mvn \
  -Dbenchmark.useOjp=true \
  -Dbenchmark.requestCount=1000 \
  -Dbenchmark.db.host=localhost \
  -Dbenchmark.db.port=5432 \
  -Dbenchmark.db.name=benchmark \
  -Dbenchmark.db.user=postgres \
  -Dbenchmark.ojp.host=localhost \
  -Dbenchmark.ojp.port=1059 \
  exec:java
```

## Notes

- The benchmark recreates its schema and populates realistic seed data before each run.
- The benchmark drops and recreates the `open_loop_benchmark` schema on every run, so use a dedicated PostgreSQL database and do not point it at shared data you need to keep.
- Setup and warm-up are excluded from measured benchmark time.
- The workload mix is controlled by constants in `OpenLoopJdbcBenchmark`.
- Run `mvn exec:java -Dexec.args=--help` to print available system properties.
