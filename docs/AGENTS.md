## Agent Instructions for `data-reconciliation-framework` (Scala/Spark)

This document provides guidance for AI agents working on the Scala/Spark `data-reconciliation-framework` codebase.

### 1. Core Design Principles:

*   **Modularity:** The framework is organized into distinct Scala packages and classes/objects (e.g., `services`, `readers`, `models`, `config`). Strive to maintain this separation of concerns. New functionalities should fit logically into existing structures or warrant new ones.
*   **Configuration Driven:** Reconciliation jobs are defined by configurations fetched directly from a database via JDBC. These configurations are mapped to Scala case classes (see `ReconciliationConfig.scala`). Avoid hardcoding job-specific logic. Enhancements should generally be controllable via these configurations.
*   **Spark Native & DataFrame API:** Prioritize Spark's DataFrame and Dataset APIs for data manipulation to leverage Spark's optimization and distributed processing capabilities. Use Spark SQL where appropriate.
*   **Immutability:** Embrace immutability, a core tenet of Scala and functional programming. Case classes should be immutable. DataFrame transformations should produce new DataFrames rather than modifying existing ones in place.
*   **Type Safety:** Leverage Scala's strong type system to catch errors at compile time. Use `Option` for optional values, and `Try` or `Either` for operations that can fail.
*   **Clarity and Logging:** Write clear, concise, and well-commented Scala code. Implement thorough logging using SLF4J with Logback (configured via `logback.xml`) to aid in debugging, especially in a distributed Spark environment.

### 2. Working with Key Modules/Packages:

*   **`com.example.reconciler.Main`**: The entry point of the Spark application. It orchestrates fetching job configurations via JDBC (potentially multiple), iterating through them, setting up Spark, calling services for each job, and handling top-level errors.
*   **`com.example.reconciler.config`**:
    *   Contains Scala case classes (`ReconciliationJobConfig`, `DataSourceConfig`, `HiveOutputConfig`, etc.) that define the structure of the job configuration.
    *   `JdbcConfigFetcher`: Responsible for fetching job configurations from a database via JDBC and mapping `ResultSet` data (including parsing JSON strings for complex parts) to the Scala case classes.
*   **`com.example.reconciler.readers`**:
    *   `DataSourceReader`: A trait defining the contract for reading data.
    *   `FileDataSourceReader`, `HiveDataSourceReader`: Implementations for different source types.
    *   `DataSourceReaderFactory`: Selects the appropriate reader based on the configuration.
    *   `SparkSchemaConverter`: Utility for converting custom schema configurations into Spark `StructType`.
*   **`com.example.reconciler.services`**:
    *   `ReconciliationService`: Contains the core logic for all reconciliation steps (row count, schema comparison, data matching by key, column value comparison, business rule execution).
    *   `OutputService`: Handles writing reconciliation results (summary and details) to HDFS and Hive tables.
    *   `EmailService`: Manages the generation and sending of HTML email reports.
*   **`com.example.reconciler.models`**: Defines Scala case classes (`ReconciliationJobSummary`, `ReconStatus`, `RowCountReconResult`, etc.) used to represent the state, results, and various components of a reconciliation job.

### 3. Configuration:

*   Job configurations are fetched from a database via JDBC. The `JdbcConfigFetcher` uses a configurable SQL query (from `RECON_JOBS_SQL_QUERY` env var) to retrieve rows.
*   Each row is mapped to a `ReconciliationJobConfig` Scala case class.
    *   Simple fields are mapped directly from ResultSet columns.
    *   Complex nested objects (like `sourceConfig`, `targetConfig`) and sequences (like `columnsToCompare`, `businessRules`) are expected to be stored as **JSON strings** in dedicated columns within the ResultSet. `JdbcConfigFetcher` then parses these JSON strings.
*   Refer to `ReconciliationConfig.scala` for the canonical structure of `ReconciliationJobConfig` and its nested case classes.
*   **JDBC Connection Details**: These are provided via environment variables:
    *   `RECON_JOBS_JDBC_URL`
    *   `RECON_JOBS_JDBC_USER`
    *   `RECON_JOBS_JDBC_PASSWORD`
    *   `RECON_JOBS_JDBC_DRIVER` (defaults to Oracle)
    *   `RECON_JOBS_SQL_QUERY`
*   **Iterative Processing**: If the `RECON_JOBS_SQL_QUERY` returns multiple rows, `Main.scala` will iterate through each, processing it as an independent reconciliation job.
*   Key configuration items within `ReconciliationJobConfig` (relevant to recent features):
    *   `hiveOutput.detailTableName`: Optional. Enables logging to the detailed events table.
    *   Options within `DataSourceConfig` for new source types or parameters.

### 4. Testing (`src/test/scala/`):

*   **Primary Framework**: ScalaTest should be used for writing unit and integration tests.
*   **Spark Testing**: Utilize `com.holdenkarau:spark-testing-base` for utilities that simplify testing Spark code (e.g., `DataFrameSuiteBase` for easy SparkSession management and DataFrame comparisons).
*   **Test Coverage**:
    *   Write unit tests for individual methods and classes, especially for business logic in services and complex transformations.
    *   Mock dependencies where appropriate (e.g., mocking API calls in `OracleConfigFetcher` tests, or mocking email sending).
    *   For `ReconciliationService`, create small, targeted DataFrames to test specific reconciliation scenarios (matches, mismatches, various data types, nulls, tolerances).
    *   For `OutputService`, test the schema and content of DataFrames prepared for Hive/HDFS.
*   Run tests using Maven: `mvn test`.

### 5. Spark Best Practices (Scala Context):

*   **Avoid `collect()` on Large DataFrames:** Only use for very small datasets (e.g., results of aggregations known to be small) or in test scenarios.
*   **Caching/Persisting (`.cache()`, `.persist()`):** Use strategically on DataFrames that are accessed multiple times. Remember to `unpersist()` when no longer needed, though Spark often manages this well.
*   **Schema Definition:** Use explicit schemas (`StructType`) when reading data where possible, rather than relying on schema inference, for robustness and performance.
*   **UDFs (User Defined Functions):** While powerful, Scala UDFs can have performance overhead. Prefer built-in Spark SQL functions when available. If UDFs are necessary, ensure they are efficient.
*   **Partitioning:** Be mindful of data partitioning, especially before joins or writes. Use `repartition()` or `coalesce()` as needed. `coalesce(1)` can be useful for writing single output files for small results.
*   **Serialization:** Ensure any custom classes or objects used in RDD/DataFrame operations are serializable.

### 6. Build and Dependency Management (Maven):

*   The `pom.xml` file is the central configuration for the build process and dependencies.
*   Key Maven commands:
    *   `mvn clean compile`: Compile the source code.
    *   `mvn test`: Run unit and integration tests.
    *   `mvn package`: Create the application JAR (e.g., for `spark-submit`).
    *   `mvn clean install`: Compile, test, package, and install the artifact into the local Maven repository.

### 7. Code Style and Conventions (Scala):

*   Follow standard Scala coding conventions (e.g., naming, formatting). Consider using Scalafmt for automated formatting if a team adopts it.
*   Leverage case classes for immutable data structures.
*   Use `Option` to handle optional values explicitly, avoiding nulls where possible.
*   Employ functional programming constructs (map, filter, foreach, for-comprehensions) for clarity and conciseness with collections and DataFrames.
*   Write clear and concise comments and Scaladocs, especially for public APIs and complex logic.

### 8. Specific Instructions / Considerations:

*   **Error Handling:**
    *   Use `Try`, `Option`, or `Either` for functions that can fail or return optional results.
    *   Allow critical exceptions to propagate to `Main.scala` to be caught, logged, and to result in a job failure status.
*   **Hive Interaction:**
    *   Be mindful of Hive table schemas when writing DataFrames.
    *   Use appropriate `SaveMode` (e.g., `Append`, `Overwrite`).
*   **Logging:** Use the SLF4J loggers provided or instantiated within classes. Log important events, decisions, errors, and key data points (like counts) to aid diagnostics.
*   **Configuration Data Source Changes**: If the structure of `ReconciliationJobConfig` changes, or if the way data is stored in the configuration database (e.g., column names for JSON strings, or structure of those JSONs) changes, ensure `JdbcConfigFetcher`'s mapping logic is updated accordingly.

### 9. Workflow for Making Changes:

1.  **Understand Requirements:** Clearly grasp the feature or bug fix.
2.  **Identify Affected Modules:** Determine which Scala packages/classes will need modification.
3.  **Implement Changes:** Write or modify Scala code, adhering to the principles above.
4.  **Write/Update Tests:** Create new ScalaTest tests or update existing ones to cover the changes.
5.  **Local Testing:** Run `mvn test` to ensure all tests pass. Manually test the flow if applicable using `spark-submit` with a sample configuration.
6.  **Commit:** Use clear, conventional commit messages. Reference issue numbers if applicable.

By adhering to these guidelines, AI agents can contribute effectively and maintain the quality of the `data-reconciliation-framework`.
