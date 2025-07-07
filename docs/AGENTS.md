## Agent Instructions for `data-reconciliation-framework`

This document provides guidance for AI agents working on the `data-reconciliation-framework` codebase.

### 1. Core Design Principles:

*   **Modularity:** The framework is divided into distinct modules (readers, reconcilers, writers, reporting, utils). Maintain this separation of concerns. New functionalities should fit into existing modules or warrant new ones if significantly different.
*   **Configuration Driven:** All reconciliation jobs are defined by external configuration files (JSON/YAML). Avoid hardcoding job-specific logic. Enhancements should ideally be controllable via configuration.
*   **Spark Native:** Prioritize Spark DataFrame operations for performance and scalability. Minimize use of pandas for large datasets unless it's for operations not easily done in Spark (like some Excel interactions, but even then, prefer Spark-native solutions like `spark-excel` if feasible for production).
*   **Immutability:** Treat DataFrames as immutable. Transformations should produce new DataFrames.
*   **Clarity and Logging:** Ensure code is well-commented, and logging is thorough to help debug issues in a distributed Spark environment.

### 2. Working with Modules:

*   **`readers`**: When adding support for new file formats, create a new function or extend `read_data` logic within `data_reader.py`. Ensure schema handling (inference and explicit) is consistent.
*   **`reconcilers`**: The `Reconciler` class contains the core comparison logic. Changes here should be carefully tested for performance and correctness, especially concerning join strategies, null handling, and tolerance comparisons.
*   **`writers`**: For new output targets or formats, extend `DataWriter`. Ensure options for partitioning, mode (`overwrite`, `append`), and format-specific settings are configurable.
*   **`reporting`**: `ReportGenerator` creates DataFrame representations of results. If new report structures are needed, add methods here. For HTML or other presentation formats (like email), use templating (e.g., Jinja2 in `EmailNotifier`).
*   **`utils`**:
    *   `config_loader.py`: Handles loading and initial parsing of job configs. If new global config sections or complex schema resolution logic is needed, modify it here.
    *   `email_utils.py`: Manages email notifications. Keep SMTP logic and template rendering encapsulated.
*   **`main.py`**: Orchestrates the flow. Changes here are typically to integrate new steps or modules into the main pipeline.

### 3. Configuration (`config/`):

*   The primary job configuration is `sample_job_config.json`. When adding new features, consider how they will be configured here.
*   Schema definitions can be inline or in separate JSON files. Ensure `config_loader.py` correctly resolves paths.
*   Email templates are in `config/email_templates/`.

### 4. Testing (`tests/`):

*   **Unit Tests:** Each module should have corresponding unit tests. Use `unittest` or `pytest`.
    *   Mock SparkSession where appropriate for focused tests.
    *   For reader/writer tests, create temporary small sample files.
    *   For reconciler tests, create small, targeted DataFrames to test specific scenarios (matches, mismatches, nulls, tolerances, edge cases).
*   **Integration Tests:** (Conceptual for now, but good to keep in mind) Tests that run a minimal end-to-end flow with a sample configuration and data could be beneficial.
*   **Test Coverage:** Strive for good test coverage for any new code or modifications.

### 5. Spark Best Practices:

*   **Avoid `collect()` on large DataFrames:** Only use for small datasets or in test scenarios.
*   **Caching/Persisting:** Use `df.persist()` or `df.cache()` judiciously on DataFrames that are used multiple times in an iterative process (like the `joined_df` or `in_both_df` in `Reconciler` if they were accessed more frequently). Remember to `unpersist()` when done.
*   **Broadcasting:** For joins where one DataFrame is significantly smaller, consider broadcasting it.
*   **Partitioning:** Be mindful of data partitioning, especially before writes or shuffles. Use `repartition()` or `coalesce()` as appropriate. `coalesce(1)` is good for small output files like summaries.
*   **Schema Management:** Define schemas explicitly where possible to avoid performance overhead and potential errors from schema inference.
*   **Logging:** Use the provided `logger` instances within each module. Log key steps, counts, and configurations.

### 6. Code Style and Conventions:

*   Follow PEP 8 for Python code.
*   Use type hints.
*   Ensure clear and concise function/method names and docstrings.

### 7. Specific Instructions for this Framework:

*   **DAT Files:** The current `DataReader` treats `.dat` files like generic CSVs. If specific DAT file formats (e.g., fixed-width or proprietary binary) need to be supported, a dedicated parsing logic will be required. This might involve using specific libraries or custom RDD processing.
*   **Parameterized SQL:** The `DataReader` has a placeholder for `sql_query` in Hive source config. Parameter substitution logic needs to be robust if it's expanded (e.g., using `config.get("query_params")`).
*   **Schema Evolution:** The framework currently assumes schemas are relatively stable for a given job run. Handling significant schema evolution or dynamic schema mapping between sources would be a major enhancement.
*   **Error Handling:** Ensure robust error handling, especially for I/O operations, configuration parsing, and Spark job failures. Log errors clearly.

By following these guidelines, you can contribute effectively to the development and maintenance of the `data-reconciliation-framework`.
