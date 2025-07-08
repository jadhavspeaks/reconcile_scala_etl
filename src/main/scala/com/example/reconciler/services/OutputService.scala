package com.example.reconciler.services

import com.example.reconciler.config.{HdfsOutputConfig, HiveOutputConfig, ReconciliationJobConfig}
import com.example.reconciler.models.ReconciliationJobSummary
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._ // Added for StructField, StringType etc.
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats, FieldSerializer}
import scala.util.Try // Added for Try
import com.example.reconciler.config.FileFormat // For EnumNameSerializer
import com.example.reconciler.config.{SourceFileConfig, SourceHiveTableConfig} // For FieldSerializer in formats

class OutputService(implicit spark: SparkSession) {

  implicit val formats: Formats = DefaultFormats +
    new org.json4s.ext.EnumNameSerializer(FileFormat) +
    FieldSerializer[SourceFileConfig]() +
    FieldSerializer[SourceHiveTableConfig]() // Needed for ReconciliationJobConfig -> JSON

  /**
   * Saves reconciliation output DataFrames and summary to HDFS.
   */
  def saveToHdfs(
    summary: ReconciliationJobSummary,
    mismatchesDf: Option[DataFrame],
    sourceOnlyDf: Option[DataFrame],
    targetOnlyDf: Option[DataFrame],
    config: HdfsOutputConfig
  ): Unit = {
    println(s"INFO: Attempting to save results to HDFS path: ${config.path}")
    Try {
      // Save summary object as JSON
      // Convert summary to a DataFrame to save it easily, or write directly as JSON string
      // For simplicity, write as JSON string to a file.
      val summaryJson = Serialization.writePretty(summary)
      val summaryPath = s"${config.path}/summary/recon_summary_${summary.jobId}.json"

      // Create a single-element RDD from the JSON string and save it
      // This is a bit of a workaround to use Spark's FS APIs for single file write,
      // alternative is to use Hadoop FS API directly.
      spark.sparkContext.parallelize(Seq(summaryJson), 1).saveAsTextFile(summaryPath)
      println(s"INFO: Reconciliation summary saved to HDFS: $summaryPath")

      val saveOptions = Map("header" -> "true") // Common options

      mismatchesDf.foreach { df =>
        val path = s"${config.path}/mismatches"
        println(s"INFO: Saving mismatched records to HDFS: $path (Format: ${config.format})")
        df.write.mode(SaveMode.Overwrite).options(saveOptions).format(config.format).save(path)
      }
      sourceOnlyDf.foreach { df =>
        val path = s"${config.path}/source_only"
        println(s"INFO: Saving source-only records to HDFS: $path (Format: ${config.format})")
        df.write.mode(SaveMode.Overwrite).options(saveOptions).format(config.format).save(path)
      }
      targetOnlyDf.foreach { df =>
        val path = s"${config.path}/target_only"
        println(s"INFO: Saving target-only records to HDFS: $path (Format: ${config.format})")
        df.write.mode(SaveMode.Overwrite).options(saveOptions).format(config.format).save(path)
      }
      println(s"INFO: Successfully saved all configured outputs to HDFS: ${config.path}")
    } match {
      case scala.util.Failure(ex) =>
        println(s"ERROR: Failed to save results to HDFS: ${ex.getMessage}")
        ex.printStackTrace()
      case _ => // Success
    }
  }

  /**
   * Saves reconciliation summary and detailed mismatch DataFrame to Hive tables.
   */
  def saveToHive(
    summary: ReconciliationJobSummary,
    mismatchesDf: Option[DataFrame], // DataFrame of mismatched records with details
    // sourceOnlyDf: Option[DataFrame], // Optional: if also configured to save these to Hive
    // targetOnlyDf: Option[DataFrame], // Optional
    config: HiveOutputConfig
  ): Unit = {
    println(s"INFO: Attempting to save results to Hive database: ${config.databaseName}")
    Try {
      // Save summary: Convert summary to a DataFrame (single row)
      // This requires a defined schema for the summary table or careful construction.
      // For simplicity, we'll focus on the mismatchesDf first.
      // A proper summary table would require flattening parts of ReconciliationJobSummary or storing complex types.

      // Example: Saving a simplified summary (jobId, jobName, status, startTime, endTime)
      // In a real scenario, the Hive summary table schema must exist.
      val simpleSummaryData = Seq(
        (summary.jobId, summary.jobName, summary.overallStatus.toString, summary.startTime, summary.endTime.getOrElse(null))
      )
      val summarySchemaFields = Seq(
        StructField("job_id", StringType, false),
        StructField("job_name", StringType, true),
        StructField("overall_status", StringType, true),
        StructField("start_time_ms", LongType, true),
        StructField("end_time_ms", LongType, true) // Nullable for endTime
      )
      // We'd need to add more fields from the summary object here.
      // For now, this is a very simplified version.
      val summaryDf = spark.createDataFrame(simpleSummaryData).toDF(summarySchemaFields.map(_.name):_*)

      val summaryTableName = s"${config.databaseName}.${config.summaryTableName}"
      println(s"INFO: Saving reconciliation summary to Hive table: $summaryTableName")
      summaryDf.write.mode(SaveMode.Append).format("hive").saveAsTable(summaryTableName) // Append or Overwrite based on need


      mismatchesDf.foreach { df =>
        val mismatchTableName = s"${config.databaseName}.${config.mismatchTableName}"
        println(s"INFO: Saving mismatched records details to Hive table: $mismatchTableName")
        // Ensure the schema of mismatchesDf is compatible with the Hive table.
        // The mismatchesDf from compareColumnValues has PKs and an array of structs for mismatch_details.
        // This might need to be flattened or stored in a table that supports complex types.
        // For simplicity, if the table expects PKs and a stringified version of mismatch_details:
        // val reportableMismatchesDf = df.withColumn("mismatches_str", to_json(col("mismatches")))
        //   .drop("mismatches")
        // reportableMismatchesDf.write.mode(SaveMode.Overwrite).format("hive").saveAsTable(mismatchTableName)
        // For now, assuming direct save is possible or table schema matches df.
        df.write.mode(SaveMode.Overwrite).format("hive").saveAsTable(mismatchTableName)
      }
       println(s"INFO: Successfully saved outputs to Hive: ${config.databaseName}")
    } match {
      case scala.util.Failure(ex) =>
        println(s"ERROR: Failed to save results to Hive: ${ex.getMessage}")
        ex.printStackTrace()
      case _ => // Success
    }
  }
}
