package com.example.reco.config

import org.apache.spark.sql.types.StructType

/**
 * Represents the configuration for a data source (file or Hive table).
 * This is a simplified stub for now.
 */
case class SourceConfig(
  name: String, // User-friendly name/alias for the source
  sourceType: String, // "file", "hive"
  format: Option[String] = None, // For file type: "csv", "parquet", "json", etc.
  path: Option[String] = None, // For file type: path to file/directory
  tableName: Option[String] = None, // For hive type: "database.table"
  options: Map[String, String] = Map.empty, // Spark reader/writer options
  schema: Option[StructType] = None, // Optional explicit schema
  sqlQuery: Option[String] = None // Optional SQL query for Hive
)

/**
 * Represents reconciliation rules.
 * Simplified stub.
 */
case class ReconciliationConfig(
  keyColumns: Seq[String],
  // More rules like columns to compare, tolerances, etc., will be added later.
  numericTolerance: Option[Double] = Some(0.0) // Default to exact match for numbers
)

/**
 * Represents output targets for reconciliation results.
 * Simplified stub.
 */
case class OutputConfig(
  outputPath: String, // Base path for HDFS, or could be Hive DB for Hive outputs
  summaryFormat: String = "console", // "console", "csv", "hive"
  detailFormat: String = "console" // "console", "csv", "parquet", "hive"
  // More specific output targets (mismatches, only_in_A, etc.) can be detailed later.
)

/**
 * Main application/job configuration.
 * This is a simplified stub. Full implementation will likely parse from a file (JSON, HOCON).
 */
case class AppConfig(
  jobName: String,
  sourceA: SourceConfig,
  sourceB: SourceConfig,
  reconciliation: ReconciliationConfig,
  output: OutputConfig
  // emailNotification: Option[EmailConfig] // Placeholder for email settings
)

/**
 * Placeholder for a configuration loader object.
 * Actual implementation will read from a file and populate AppConfig.
 */
object ConfigLoader {
  def load(configPath: String): Either[String, AppConfig] = {
    // This is a STUB. In a real scenario, this would parse a config file (JSON, HOCON, etc.)
    // For now, returning a hardcoded sample config for demonstration purposes.
    println(s"Stub: Attempting to load configuration from '$configPath' (not really, returning sample).")

    val sampleSourceA = SourceConfig(name = "SourceFile", sourceType = "file", format = Some("csv"), path = Some("data/sample_source.csv"), options = Map("header" -> "true", "inferSchema" -> "true"))
    val sampleSourceB = SourceConfig(name = "TargetHiveTable", sourceType = "hive", tableName = Some("raw.target_table"))
    val sampleReconConfig = ReconciliationConfig(keyColumns = Seq("id"))
    val sampleOutputConfig = OutputConfig(outputPath = "/reports/recon_output", summaryFormat = "console", detailFormat = "console")

    Right(AppConfig(
      jobName = "SampleReconJob",
      sourceA = sampleSourceA,
      sourceB = sampleSourceB,
      reconciliation = sampleReconConfig,
      output = sampleOutputConfig
    ))
  }
}
