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
    *   Contains Scala case classes (`ReconciliationJobConfig`, `BusinessRuleConfig`, `DataSourceConfig`, etc.).
    *   `JdbcConfigFetcher`: Fetches configurations from a database. It expects a **fully flattened ResultSet** where all parameters are string columns. It performs:
        *   Direct string-to-type conversions.
        *   Manual construction of nested objects (e.g., `DataSourceConfig`, `EmailConfig`).
        *   Parsing of comma-separated strings for sequences (e.g., `primaryKeyColumns`, `columnsToCompare` names, `emailRecipients`).
        *   Derivation of `columnNameMapping: Map[String, String]` from two dedicated comma-separated string columns (`source_mapping_columns_str`, `target_mapping_columns_str`).
        *   Conditional logic for business rules based on `checkBusinessTransformation` (legacy rule vs. literal) and `businessRuleComparisonFlag` (new rule: SQL output vs. target table, using `joinKeysForTargetComparison`).
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
*   Each row from the ResultSet is mapped to a `ReconciliationJobConfig` Scala case class. The ResultSet must provide a **fully flattened structure** with all parameters as individual string columns.
    *   **Direct Mapping**: Simple fields (e.g., `jobId`, flags like `sourceToTargetFlag`) are read as strings and converted to their target types (Boolean, Int, Double, etc.).
    *   **Nested Object Construction**: Objects like `DataSourceConfig`, `HdfsOutputConfig`, etc., are manually constructed by `JdbcConfigFetcher` reading their constituent fields from multiple dedicated string columns (e.g., `source_type`, `source_file_path`, `source_file_format` for a source file).
    *   **Column Name Mapping (`columnNameMapping`)**: This `Option[Map[String, String]]` is derived by `JdbcConfigFetcher` from two specific database columns: one containing a comma-separated list of source column names (e.g., `source_mapping_columns_str`) and another for target column names (e.g., `target_mapping_columns_str`). These lists must correspond positionally.
    *   **Sequence Parsing (from comma-separated strings)**:
        *   `primaryKeyColumns` (`Seq[String]`): Parsed from a single string column (e.g., `pk_columns_str`). These should be target column names (or source names if not mapped).
        *   `columnsToCompare` (`Seq[ReconColumnConfig]`): The names are parsed from a single string column (e.g., `compare_column_names_str`). Each results in a `ReconColumnConfig` with default comparison attributes. Column names should be target names.
        *   `emailNotifications.recipients` (`Seq[String]`).
        *   `BusinessRuleConfig.joinKeysForTargetComparison` (`Option[Seq[String]]`).
    *   **Business Rule Handling (Two Modes)**:
        *   **Legacy (vs. Literal)**: Controlled by `check_business_transformation` flag (string "Yes"/"No" column). If "Yes", then `business_rule_name`, `business_rule_sql`, and `business_rule_expected_result` string columns define a single `BusinessRuleConfig` for this mode.
        *   **New (SQL Result vs. Target Table)**: Controlled by `business_rule_comparison_flag` (string "true"/"false" column). If "true", it uses the same `business_rule_name`, `business_rule_sql`. A `business_rule_target_join_keys_str` column (comma-separated target column names) specifies join keys. The main `jobConfig.columnsToCompare` are reused for value checks. The SQL output for this rule is expected to align with source data structure (and will be mapped using `columnNameMapping` if provided).
*   Refer to `ReconciliationConfig.scala` for the case class structures and `JdbcConfigFetcher.scala` for the expected constant names for database columns.
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
*   **Configuration Data Source Changes**: If `ReconciliationJobConfig` structure changes, or if the column names/expected string formats in the configuration database change, `JdbcConfigFetcher.scala`'s mapping logic (including column name constants and parsing functions) must be updated.

### 9. Workflow for Making Changes:

1.  **Understand Requirements:** Clearly grasp the feature or bug fix.
2.  **Identify Affected Modules:** Determine which Scala packages/classes will need modification.
3.  **Implement Changes:** Write or modify Scala code, adhering to the principles above.
4.  **Write/Update Tests:** Create new ScalaTest tests or update existing ones to cover the changes.
5.  **Local Testing:** Run `mvn test` to ensure all tests pass. Manually test the flow if applicable using `spark-submit` with a sample configuration.
6.  **Commit:** Use clear, conventional commit messages. Reference issue numbers if applicable.

By adhering to these guidelines, AI agents can contribute effectively and maintain the quality of the `data-reconciliation-framework`.
