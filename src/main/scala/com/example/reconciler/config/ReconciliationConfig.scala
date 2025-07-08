package com.example.reconciler.config

object FileFormat extends Enumeration {
  type FileFormat = Value
  val CSV, EXCEL, TEXT, DAT, PARQUET, ORC, JSON = Value // Added common Hadoop formats too
}

import FileFormat._

case class SchemaColumnConfig(
  name: String,
  dataType: String, // Spark SQL data type string, e.g., "StringType", "IntegerType", "TimestampType"
  nullable: Boolean = true,
  format: Option[String] = None // For date/timestamp formats
)

case class FileSourceConfig(
  path: String,
  format: FileFormat,
  delimiter: Option[String] = None, // For CSV, DAT, TEXT
  header: Option[Boolean] = None,   // For CSV, EXCEL
  sheetName: Option[String] = None, // For EXCEL
  multiLine: Option[Boolean] = None, // For JSON
  customSchema: Option[Seq[SchemaColumnConfig]] = None, // Explicit schema
  inferSchema: Option[Boolean] = Some(false)
)

case class HiveSourceConfig(
  databaseName: String,
  tableName: String
)

sealed trait DataSourceConfig {
  def sourceType: String // "file" or "hive"
}
case class SourceFileConfig(fileConfig: FileSourceConfig) extends DataSourceConfig {
  override def sourceType: String = "file"
}
case class SourceHiveTableConfig(hiveConfig: HiveSourceConfig) extends DataSourceConfig {
  override def sourceType: String = "hive"
}

case class ReconColumnConfig(
  columnName: String,
  isPrimaryKey: Boolean = false,
  tolerance: Option[Double] = None, // For numeric comparisons
  ignoreCase: Option[Boolean] = Some(false), // For string comparisons
  trimWhitespace: Option[Boolean] = Some(true) // For string comparisons
)

case class BusinessRuleConfig(
  ruleName: String,
  sqlQuery: String, // Parameterized SQL query
  expectedResult: Option[String] // Or a more complex structure for expected results
)

case class HdfsOutputConfig(
  path: String,
  format: String = "parquet" // e.g., parquet, orc, csv
)

case class HiveOutputConfig(
  databaseName: String,
  summaryTableName: String,
  mismatchTableName: String
)

case class EmailConfig(
  enabled: Boolean = true,
  recipients: Seq[String],
  subjectPrefix: String = "[Recon Report]",
  smtpHost: String,
  smtpPort: Int,
  smtpUser: Option[String] = None,
  smtpPassword: Option[String] = None,
  starttlsEnabled: Option[Boolean] = Some(true)
)

case class ReconciliationJobConfig(
  jobId: String,
  jobName: String,
  sourceConfig: DataSourceConfig,
  targetConfig: DataSourceConfig,
  primaryKeyColumns: Seq[String], // Column names used for joining/identifying records
  columnsToCompare: Seq[ReconColumnConfig], // Detailed config for columns to be compared
  businessRules: Option[Seq[BusinessRuleConfig]] = None,
  performRowCountCheck: Boolean = true,
  performSchemaCheck: Boolean = true,
  performDataReconciliation: Boolean = true,
  hdfsOutput: Option[HdfsOutputConfig] = None,
  hiveOutput: Option[HiveOutputConfig] = None,
  emailNotifications: Option[EmailConfig] = None,
  // Advanced options
  sampleMismatchLimit: Int = 100, // Max number of mismatches to include in detailed report
  errorTolerancePercentage: Option[Double] = None // If overall error % is above this, maybe fail the job
)

/**
 * Placeholder for the Oracle Configuration Fetcher.
 * This would involve using JDBC to connect to Oracle, execute a query/stored procedure,
 * and map the results to the ReconciliationJobConfig case class.
 */
object OracleConfigFetcher {
  /**
   * Fetches the reconciliation job configuration from an Oracle database.
   *
   * This is a placeholder implementation. The actual implementation would require:
   * - Oracle JDBC driver on the classpath.
   * - Connection details for the Oracle database (could be part of a separate app config).
   * - SQL query or stored procedure name to fetch the configuration.
   * - Logic to parse the ResultSet and populate the ReconciliationJobConfig object.
   *   This might involve mapping columns from a config table to the case class fields.
   *   Complex configurations (like nested Seq of columns) might be stored as JSON/XML in Oracle
   *   or across multiple related tables.
   *
   * @param reconJobId The ID of the reconciliation job to fetch.
   * @param sparkSession Implicit SparkSession, useful if config is in a table Spark can read.
   * @return Option[ReconciliationJobConfig]
   */
  def fetchConfig(reconJobId: String)(implicit sparkSession: org.apache.spark.sql.SparkSession): Option[ReconciliationJobConfig] = {
    println(s"INFO: Attempting to fetch configuration for job ID: $reconJobId (Placeholder Implementation)")

    // Example: Simulating fetching a config.
    // In a real scenario, this would be JDBC calls, parsing JSON from a column, etc.
    if (reconJobId == "sampleReconJob1") {
      Some(
        ReconciliationJobConfig(
          jobId = "sampleReconJob1",
          jobName = "Sample CSV to Hive Reconciliation",
          sourceConfig = SourceFileConfig(FileSourceConfig(
            path = "hdfs:///user/data/input/sample_source.csv",
            format = FileFormat.CSV,
            delimiter = Some(","),
            header = Some(true),
            inferSchema = Some(true)
            // customSchema = Some(Seq(
            //   SchemaColumnConfig("id", "IntegerType"),
            //   SchemaColumnConfig("name", "StringType"),
            //   SchemaColumnConfig("value", "DoubleType"),
            //   SchemaColumnConfig("event_date", "DateType", format = Some("yyyy-MM-dd"))
            // ))
          )),
          targetConfig = SourceHiveTableConfig(HiveSourceConfig(
            databaseName = "raw_db",
            tableName = "source_mirror_table"
          )),
          primaryKeyColumns = Seq("id"),
          columnsToCompare = Seq(
            ReconColumnConfig("id", isPrimaryKey = true),
            ReconColumnConfig("name", ignoreCase = Some(true), trimWhitespace = Some(true)),
            ReconColumnConfig("value", tolerance = Some(0.001)),
            ReconColumnConfig("event_date")
          ),
          performRowCountCheck = true,
          performSchemaCheck = true,
          performDataReconciliation = true,
          hdfsOutput = Some(HdfsOutputConfig(
            path = "hdfs:///user/data/recon_output/sampleReconJob1"
          )),
          hiveOutput = Some(HiveOutputConfig(
            databaseName = "recon_db",
            summaryTableName = "recon_summary_sampleReconJob1",
            mismatchTableName = "recon_mismatches_sampleReconJob1"
          )),
          emailNotifications = Some(EmailConfig(
            recipients = Seq("user1@example.com", "user2@example.com"),
            smtpHost = "smtp.example.com",
            smtpPort = 587
          )),
          sampleMismatchLimit = 200
        )
      )
    } else {
      println(s"ERROR: No configuration found for job ID: $reconJobId")
      None
    }
  }
}
