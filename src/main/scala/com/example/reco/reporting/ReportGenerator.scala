package com.example.reco.reporting

import com.example.reco.RowCountSummary
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Trait for generating reconciliation reports.
 */
trait ReportGenerator {
  /**
   * Generates a summary report for row counts.
   * @param summary The RowCountSummary object.
   */
  def generateRowCountSummaryReport(summary: RowCountSummary): Unit

  /**
   * Generates a detailed report of mismatched records.
   * @param mismatchedData DataFrame containing records that have discrepancies.
   * @param outputPath Path to store the report.
   * @param format Format of the report (e.g., "csv", "html", "txt").
   */
  def generateMismatchDetailReport(mismatchedData: DataFrame, outputPath: String, format: String = "csv"): Unit

  /**
   * Generates a report for records only present in Source A.
   * @param onlyInA DataFrame containing records only in Source A.
   * @param outputPath Path to store the report.
   * @param format Format of the report.
   */
  def generateOnlyInSourceAReport(onlyInA: DataFrame, outputPath: String, format: String = "csv"): Unit

  /**
   * Generates a report for records only present in Source B.
   * @param onlyInB DataFrame containing records only in Source B.
   * @param outputPath Path to store the report.
   * @param format Format of the report.
   */
  def generateOnlyInSourceBReport(onlyInB: DataFrame, outputPath: String, format: String = "csv"): Unit
}

/**
 * A basic implementation of ReportGenerator that prints to console
 * and prepares for future HDFS/Hive/Email output.
 */
class BasicReportGenerator(implicit spark: SparkSession) extends ReportGenerator {

  override def generateRowCountSummaryReport(summary: RowCountSummary): Unit = {
    println("--- Row Count Summary Report ---")
    println(summary.toString)
    // Future: Write to HDFS/Hive, send email
  }

  override def generateMismatchDetailReport(mismatchedData: DataFrame, outputPath: String, format: String = "csv"): Unit = {
    println(s"--- Mismatch Detail Report (stub) ---")
    println(s"Output Path: $outputPath, Format: $format")
    mismatchedData.show(5, truncate = false) // Show a sample
    // Future: Implement actual writing logic (e.g., mismatchedData.write.format(format).save(outputPath))
  }

  override def generateOnlyInSourceAReport(onlyInA: DataFrame, outputPath: String, format: String = "csv"): Unit = {
    println(s"--- Records Only in Source A Report (stub) ---")
    println(s"Output Path: $outputPath, Format: $format")
    onlyInA.show(5, truncate = false) // Show a sample
    // Future: Implement actual writing logic
  }

  override def generateOnlyInSourceBReport(onlyInB: DataFrame, outputPath: String, format: String = "csv"): Unit = {
    println(s"--- Records Only in Source B Report (stub) ---")
    println(s"Output Path: $outputPath, Format: $format")
    onlyInB.show(5, truncate = false) // Show a sample
    // Future: Implement actual writing logic
  }
}

object BasicReportGenerator {
  def apply()(implicit spark: SparkSession): BasicReportGenerator = new BasicReportGenerator()
}
