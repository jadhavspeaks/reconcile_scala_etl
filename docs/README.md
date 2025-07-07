# Data Reconciliation Framework

## Overview

This framework provides a Spark-based solution for data reconciliation. It is designed to compare data from various sources (like CSV, Excel, Text files, and Hive tables) against other Hive tables (raw schema or transformed work schema). The framework performs row count checks, column-level data validation, and identifies missing, extra, or mismatched records.

It supports:
- Multiple file formats with schema inference or configuration-based schemas.
- Numeric tolerances for comparisons.
- Parameterized SQL for comparing against transformed data.
- Generation of summary and detailed mismatch reports.
- Storage of reconciliation output in Hive or HDFS.
- HTML email notifications for business users.

## Project Structure

```
data-reconciliation-framework/
├── app/                  # Core application logic
│   ├── main.py           # Main application orchestrator
│   ├── readers/          # Data ingestion modules
│   ├── reconcilers/      # Reconciliation logic
│   ├── writers/          # Output writing modules
│   ├── utils/            # Utility functions (config loading, email)
│   └── reporting/        # Report generation
├── config/               # Configuration files
│   ├── sample_job_config.json # Example job configuration
│   └── email_templates/  # HTML templates for email notifications
│       └── basic_recon_email.html
├── tests/                # Unit and integration tests
├── data/                 # Sample input files for testing/demo
├── docs/                 # Documentation
├── requirements.txt      # Python dependencies
└── .gitignore            # Git ignore file
```

## Prerequisites

- Python 3.7+
- Apache Spark 3.0+ (with Hive support enabled for Hive operations)
- Hadoop HDFS (for HDFS input/output, if used)
- Access to a Hive Metastore (if reconciling with Hive tables)
- SMTP server (for email notifications)

## Installation

1.  **Clone the repository (if applicable)**
    ```bash
    # git clone <repository_url>
    # cd data-reconciliation-framework
    ```

2.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Ensure Spark is configured:**
    Your `SPARK_HOME` environment variable should be set, and Spark binaries should be in your `PATH`, or you should submit jobs using `spark-submit` which points to your Spark installation.

## Configuration

Reconciliation jobs are defined in JSON (or YAML) configuration files. See `config/sample_job_config.json` for a detailed example.

Key sections in the job configuration:

*   `job_name`: A descriptive name for the reconciliation job.
*   `source_a`: Configuration for the first data source.
    *   `alias`: User-friendly alias for this source.
    *   `type`: `file` or `hive`.
    *   `format`: (for `file` type) `csv`, `excel`, `json`, `parquet`, `text`, `dat`.
    *   `path`: (for `file` type) Path to the data file or directory.
    *   `table_name`: (for `hive` type) Name of the Hive table.
    *   `sql_query`: (for `hive` type, optional) A SQL query to execute against Hive. Can be parameterized using `${param_name}` syntax.
    *   `query_params`: (optional) A dictionary of parameters for `sql_query`.
    *   `schema`: (optional) Schema definition. Can be:
        *   A list of column definitions: `[{"name": "col1", "type": "string", "nullable": true}, ...]`
        *   A path to a JSON file containing such a list (relative to the job config file or absolute).
        *   If omitted, schema inference will be attempted for file sources (controlled by `infer_schema` option).
    *   `options`: Spark DataFrameReader options (e.g., `header`, `delimiter`, `inferSchema`).
*   `source_b`: Configuration for the second data source (similar structure to `source_a`). Typically a Hive table.
*   `reconciliation`: Rules for how to reconcile the two sources.
    *   `key_columns`: List of column names to use as join keys for comparison.
    *   `compare_columns`: List of columns to compare. Each entry is a dictionary:
        *   `name`: Name of the column.
        *   `data_type`: Target data type for comparison (e.g., `string`, `integer`, `double`, `date`, `decimal(p,s)`). The framework will attempt to cast data to this type before comparison.
        *   `tolerance`: (optional, for numeric types like `double`, `integer`, `decimal`) Numeric tolerance for considering values as equal.
        *   `nullable`: (optional, default `true`) Whether the column can be null. (Primarily for schema definition, null comparison logic is intrinsic).
*   `output`: Configuration for storing reconciliation results.
    *   `base_path`: (for `file` type outputs) Base HDFS/local directory for output files.
    *   `hive_database`: (for `hive` type outputs) Target Hive database.
    *   `outputs`: A dictionary defining specific outputs:
        *   `summary`: Configuration for the summary report.
        *   `mismatched_data`: Configuration for detailed mismatched records.
        *   `only_in_a_data`: Configuration for records only in source A.
        *   `only_in_b_data`: Configuration for records only in source B.
        *   `perfectly_matched_data`: Configuration for records that matched perfectly.
        *   Each output can specify:
            *   `type`: `file` or `hive`.
            *   `name` (for `file`), `table_name` (for `hive`).
            *   `format` (e.g., `csv`, `parquet`, `json`, `orc`).
            *   `options` (Spark DataFrameWriter options).
            *   `enabled`: `true` or `false` (default `true`).
*   `notifications`: Configuration for email notifications.
    *   `email_settings_ref`: Reference to a global email server configuration.
    *   `send_email`: `true` or `false`.
    *   `recipients`: List of email addresses.
    *   `subject_prefix`: Prefix for the email subject.
    *   `html_template`: Name of the HTML template file (located in `template_dir` specified in email server config).
    *   `attach_summary_csv`: (Future placeholder) `true` to attach the summary report as CSV.
*   `global_configs`: Global settings, like email server details.
    *   `email_configurations`: Dictionary of named email server setups.
        *   `smtp_server`, `smtp_port`, `smtp_user`, `smtp_password`, `use_tls`, `sender_email`, `template_dir`.

### Schema Definition

The `schema` attribute for sources can be provided in multiple ways:
1.  **Inline JSON Array**:
    ```json
    "schema": [
        {"name": "id", "type": "integer", "nullable": false},
        {"name": "product_name", "type": "string"},
        {"name": "price", "type": "decimal(10,2)"}
    ]
    ```
2.  **Path to a JSON Schema File**:
    The JSON file should contain an array as shown above.
    ```json
    "schema": "path/to/your_schema.json"
    ```
    Paths can be relative to the main job configuration file or absolute.
3.  **Inferred Schema**:
    If `schema` is omitted, Spark will attempt to infer the schema for file-based sources. You can guide this with the `inferSchema: "true"` option in `source_a.options`. For Hive sources, the schema is read from the Metastore.

Supported data types for schema definition include: `string` (or `str`), `integer` (or `int`), `long`, `short`, `byte`, `double`, `float`, `decimal` (e.g., `decimal(precision,scale)`), `boolean` (or `bool`), `date`, `timestamp`.

## Usage

Run the reconciliation framework using `spark-submit`:

```bash
spark-submit app/main.py --config /path/to/your_job_config.json
```

Replace `/path/to/your_job_config.json` with the actual path to your job configuration file.

### Example `spark-submit` for a YARN cluster:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "DataRecon_JobName" \
  --py-files app.zip \ # You might need to zip your app directory and utils
  app/main.py \
  --config /path/to/your_job_config.json
```
(Adjust `--py-files` or package structure as needed for your deployment).

## Modules

*   **`app.main`**: Entry point of the application. Parses arguments, initializes Spark, and orchestrates the reconciliation process.
*   **`app.readers.data_reader`**: Handles reading data from various file formats (CSV, Excel, Parquet, JSON, Text) and Hive tables. Supports schema inference and explicit schema definition.
*   **`app.reconcilers.reconciler`**: Core logic for comparing two DataFrames. It identifies matched, mismatched, and unique records based on key columns and comparison rules (including numeric tolerances).
*   **`app.writers.data_writer`**: Writes the reconciliation results (summary, detailed mismatches, unique records) to specified outputs like HDFS files or Hive tables.
*   **`app.reporting.report_generator`**: Generates summary and detailed DataFrames from the reconciliation results.
*   **`app.utils.config_loader`**: Loads and parses job configuration files (JSON/YAML), including resolving schema definitions.
*   **`app.utils.email_utils`**: Sends HTML email notifications with reconciliation summaries.

## Development & Testing

-   Unit tests are located in the `tests/` directory.
-   To run tests (example using `unittest`):
    ```bash
    python -m unittest discover -s tests
    ```
    Ensure necessary test data or mock services (like a local SMTP debugging server for email tests) are available.

## Future Enhancements
*   Support for more complex data type coercions.
*   Advanced column mapping between sources if names differ significantly.
*   More sophisticated HTML report templating and options.
*   Integration with workflow orchestrators (e.g., Airflow).
*   Checksum or hash-based row comparison for very wide tables.
*   DAT file custom parser if it's a non-standard binary format.
*   UI for configuring jobs and viewing reports.
```
