package com.example.reconciler.services

import com.example.reconciler.config.{HdfsOutputConfig, HiveOutputConfig, ReconciliationJobConfig}
import com.example.reconciler.models.ReconciliationJobSummary
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._ // Added for StructField, StringType etc.
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats, FieldSerializer}
import org.slf4j.LoggerFactory // Added for logging
import scala.util.Try // Added for Try
import com.example.reconciler.config.FileFormat // For EnumNameSerializer
import com.example.reconciler.config.{SourceFileConfig, SourceHiveTableConfig} // For FieldSerializer in formats

class OutputService(implicit spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(getClass)

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
    logger.info(s"Attempting to save results to HDFS path: ${config.path}")
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
      logger.info(s"Reconciliation summary saved to HDFS: $summaryPath")

      val saveOptions = Map("header" -> "true") // Common options

      mismatchesDf.foreach { df =>
        val path = s"${config.path}/mismatches"
        logger.info(s"Saving mismatched records to HDFS: $path (Format: ${config.format})")
        df.write.mode(SaveMode.Overwrite).options(saveOptions).format(config.format).save(path)
      }
      sourceOnlyDf.foreach { df =>
        val path = s"${config.path}/source_only"
        logger.info(s"Saving source-only records to HDFS: $path (Format: ${config.format})")
        df.write.mode(SaveMode.Overwrite).options(saveOptions).format(config.format).save(path)
      }
      targetOnlyDf.foreach { df =>
        val path = s"${config.path}/target_only"
        logger.info(s"Saving target-only records to HDFS: $path (Format: ${config.format})")
        df.write.mode(SaveMode.Overwrite).options(saveOptions).format(config.format).save(path)
      }
      logger.info(s"Successfully saved all configured outputs to HDFS: ${config.path}")
    } match {
      case scala.util.Failure(ex) =>
        logger.error(s"Failed to save results to HDFS: ${ex.getMessage}", ex)
        // ex.printStackTrace() // Handled by logger
      case _ => // Success
    }
  }

  /**
   * Saves reconciliation summary and detailed mismatch DataFrame to Hive tables.
   */
  def saveToHive(
    summary: ReconciliationJobSummary,
    jobConfig: ReconciliationJobConfig, // Added to access source/target names etc.
    mismatchesDf: Option[DataFrame],
    // sourceOnlyDf: Option[DataFrame], // Will be handled by recon_details table
import java.sql.Timestamp
import java.util.UUID
import org.apache.spark.sql.functions.{col, explode, lit, struct, to_json} // Added for recon_details
import com.example.reconciler.models.{Failure, Success} // For explicit status check
import org.apache.spark.sql.Row // Required for creating Row objects

class OutputService(implicit spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(getClass)
  import spark.implicits._ // For toDF, etc.

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
    logger.info(s"Attempting to save results to HDFS path: ${config.path}")
    Try {
      val summaryJson = org.json4s.native.Serialization.writePretty(summary)
      val summaryPath = s"${config.path}/summary/recon_summary_${summary.jobId}.json"
      spark.sparkContext.parallelize(Seq(summaryJson), 1).saveAsTextFile(summaryPath)
      logger.info(s"Reconciliation summary saved to HDFS: $summaryPath")

      val saveOptions = Map("header" -> "true")

      mismatchesDf.foreach { df =>
        val path = s"${config.path}/mismatches"
        logger.info(s"Saving mismatched records to HDFS: $path (Format: ${config.format})")
        df.write.mode(SaveMode.Overwrite).options(saveOptions).format(config.format).save(path)
      }
      sourceOnlyDf.foreach { df =>
        val path = s"${config.path}/source_only"
        logger.info(s"Saving source-only records to HDFS: $path (Format: ${config.format})")
        df.write.mode(SaveMode.Overwrite).options(saveOptions).format(config.format).save(path)
      }
      targetOnlyDf.foreach { df =>
        val path = s"${config.path}/target_only"
        logger.info(s"Saving target-only records to HDFS: $path (Format: ${config.format})")
        df.write.mode(SaveMode.Overwrite).options(saveOptions).format(config.format).save(path)
      }
      logger.info(s"Successfully saved all configured outputs to HDFS: ${config.path}")
    } match {
      case scala.util.Failure(ex) =>
        logger.error(s"Failed to save results to HDFS: ${ex.getMessage}", ex)
      case _ => // Success
    }
  }

  private def saveReconDetailsToHive(
    summary: ReconciliationJobSummary,
    jobConfig: ReconciliationJobConfig,
    hiveOutputConfig: HiveOutputConfig,
    valueComparisonMismatchesDf: Option[DataFrame], // Renamed for clarity
    sourceOnlyDf: Option[DataFrame], // Pass sourceOnlyDf
    targetOnlyDf: Option[DataFrame]  // Pass targetOnlyDf
  )(implicit spark: SparkSession): Unit = {
    hiveOutputConfig.detailTableName.foreach { tableName =>
      val fullDetailTableName = s"${hiveOutputConfig.databaseName}.$tableName"
      logger.info(s"Preparing to save detailed reconciliation events to Hive table: $fullDetailTableName")

      val detailsToSave = new scala.collection.mutable.ListBuffer[DataFrame]()
      val currentTimestamp = new Timestamp(System.currentTimeMillis())
      val pkColumnsSeq = jobConfig.primaryKeyColumns // Seq[String]

      // Common schema for all detail types before unioning
      val commonDetailSchema = StructType(Seq(
        StructField("job_id", StringType, nullable = false),
        StructField("detail_id", StringType, nullable = false),
        StructField("detail_type", StringType, nullable = false),
        StructField("event_timestamp", TimestampType, nullable = false),
        StructField("primary_keys_json", StringType, nullable = true),
        StructField("attribute_name", StringType, nullable = true),
        StructField("source_value", StringType, nullable = true),
        StructField("target_value", StringType, nullable = true),
        StructField("remarks", StringType, nullable = true),
        StructField("additional_data_json", StringType, nullable = true)
      ))

      // 1. Schema Mismatches
      summary.schemaReconResult.filter(_.status == Failure).foreach { sr =>
        val schemaMismatchRows = sr.fieldComparisons.filter(!_.isMatch).map { fc =>
          Row(
            summary.jobId,
            UUID.randomUUID().toString,
            "SCHEMA_MISMATCH",
            currentTimestamp,
            null, // No specific PK for schema mismatch row itself
            fc.fieldName,
            fc.sourceDataType.orNull,
            fc.targetDataType.orNull,
            fc.remarks.orNull,
            null
          )
        }
        if (schemaMismatchRows.nonEmpty) {
          detailsToSave += spark.createDataFrame(spark.sparkContext.parallelize(schemaMismatchRows), commonDetailSchema)
        }
      }

      // 2. Value Comparison Mismatches
      valueComparisonMismatchesDf.foreach { df =>
        if (!df.isEmpty) {
          // df has PKs and a column "mismatches" which is an array of ColumnMismatchDetail structs
          // struct fields: columnName, sourceValue, targetValue, remark
          val explodedValueMismatches = df.withColumn("mismatch_detail", explode(col("mismatches")))
            .select(
              lit(summary.jobId).alias("job_id"),
              lit(UUID.randomUUID().toString).alias("detail_id"), // This will be the same for all rows from this batch, consider generating per row if needed
              lit("VALUE_MISMATCH").alias("detail_type"),
              lit(currentTimestamp).alias("event_timestamp"),
              to_json(struct(pkColumnsSeq.map(col): _*)).alias("primary_keys_json"),
              col("mismatch_detail.columnName").alias("attribute_name"),
              col("mismatch_detail.sourceValue").alias("source_value"),
              col("mismatch_detail.targetValue").alias("target_value"),
              col("mismatch_detail.remark").alias("remarks"),
              lit(null).cast(StringType).alias("additional_data_json") // No additional data for now
            )
            // Ensure schema matches commonDetailSchema (especially column order for unionByName)
            .select(commonDetailSchema.fieldNames.map(name => col(name)): _*)

          if (!explodedValueMismatches.isEmpty) {
            detailsToSave += explodedValueMismatches
          }
        }
      }

      // 3. Source-Only Records
      sourceOnlyDf.foreach { df =>
        if (!df.isEmpty) {
          val sourceOnlyDetails = df.select(
            lit(summary.jobId).alias("job_id"),
            lit(UUID.randomUUID().toString).alias("detail_id"), // Similar UUID generation as above
            lit("SOURCE_ONLY_KEY").alias("detail_type"),
            lit(currentTimestamp).alias("event_timestamp"),
            to_json(struct(pkColumnsSeq.map(c => col(c).alias(c)): _*)).alias("primary_keys_json"), // Ensure original PK names
            lit(null).cast(StringType).alias("attribute_name"),
            lit(null).cast(StringType).alias("source_value"),
            lit(null).cast(StringType).alias("target_value"),
            lit("Record present only in source").alias("remarks"),
            lit(null).cast(StringType).alias("additional_data_json")
          ).select(commonDetailSchema.fieldNames.map(name => col(name)): _*)

          if (!sourceOnlyDetails.isEmpty) {
            detailsToSave += sourceOnlyDetails
          }
        }
      }

      // 4. Target-Only Records
      targetOnlyDf.foreach { df =>
        if (!df.isEmpty) {
          val targetOnlyDetails = df.select(
            lit(summary.jobId).alias("job_id"),
            lit(UUID.randomUUID().toString).alias("detail_id"), // Similar UUID generation
            lit("TARGET_ONLY_KEY").alias("detail_type"),
            lit(currentTimestamp).alias("event_timestamp"),
            to_json(struct(pkColumnsSeq.map(c => col(c).alias(c)): _*)).alias("primary_keys_json"), // Ensure original PK names
            lit(null).cast(StringType).alias("attribute_name"),
            lit(null).cast(StringType).alias("source_value"),
            lit(null).cast(StringType).alias("target_value"),
            lit("Record present only in target").alias("remarks"),
            lit(null).cast(StringType).alias("additional_data_json")
          ).select(commonDetailSchema.fieldNames.map(name => col(name)): _*)

          if (!targetOnlyDetails.isEmpty) {
            detailsToSave += targetOnlyDetails
          }
        }
      }

      if (detailsToSave.nonEmpty) {
        val finalDetailsDf = detailsToSave.reduce(_ unionByName _)
        logger.info(s"Saving ${finalDetailsDf.count()} detailed events to $fullDetailTableName")
        finalDetailsDf.write.mode(SaveMode.Append).format("hive").saveAsTable(fullDetailTableName)
      } else {
        logger.info(s"No detailed events to save to $fullDetailTableName for job ${summary.jobId}")
      }
    }
  }

  def saveToHive(
    summary: ReconciliationJobSummary,
    jobConfig: ReconciliationJobConfig,
    mismatchesDfFromValueComp: Option[DataFrame], // Clarified name, this is from ValueComparisonResult
    sourceOnlyRecordsDf: Option[DataFrame], // Added for passing to details
    targetOnlyRecordsDf: Option[DataFrame], // Added for passing to details
    hiveOutputConfig: HiveOutputConfig
  ): Unit = {
    logger.info(s"Attempting to save results to Hive database: ${hiveOutputConfig.databaseName}")

    def getDataSourceName(dsConfig: DataSourceConfig): String = dsConfig match {
      case SourceFileConfig(fileCfg) => s"File: ${fileCfg.fileConfig.path}"
      case SourceHiveTableConfig(hiveCfg) => s"Hive: ${hiveCfg.hiveConfig.databaseName}.${hiveCfg.hiveConfig.tableName}"
    }

    Try {
      // 1. Save Comprehensive Summary (as implemented before)
      val summaryData = Seq(Row(
        summary.jobId, summary.jobName, summary.overallStatus.toString, summary.startTime, summary.endTime.orNull,
        summary.endTime.map(et => et - summary.startTime).orNull, getDataSourceName(jobConfig.sourceConfig),
        getDataSourceName(jobConfig.targetConfig), summary.rowCountResult.map(_.status.toString).orNull,
        summary.rowCountResult.map(_.sourceRowCount).orNull, summary.rowCountResult.map(_.targetRowCount).orNull,
        summary.rowCountResult.map(_.difference).orNull, summary.schemaReconResult.map(_.status.toString).orNull,
        summary.schemaReconResult.map(_.fieldComparisons.count(!_.isMatch)).orNull,
        summary.dataMatchingResult.map(_.status.toString).orNull,
        summary.dataMatchingResult.map(_.sourceRowCount).orNull, summary.dataMatchingResult.map(_.targetRowCount).orNull,
        summary.dataMatchingResult.map(_.matchedKeyCount).orNull, summary.dataMatchingResult.map(_.sourceOnlyKeyCount).orNull,
        summary.dataMatchingResult.map(_.targetOnlyKeyCount).orNull, summary.valueComparisonResult.map(_.status.toString).orNull,
        summary.valueComparisonResult.map(_.totalComparedRows).orNull, summary.valueComparisonResult.map(_.mismatchedRowCount).orNull,
        summary.businessRuleResults.map(brrList => if (brrList.exists(_.status == Failure)) Failure.toString else Success.toString).orNull,
        summary.businessRuleResults.map(_.size).orNull, summary.businessRuleResults.map(_.count(_.status == Failure)).orNull,
        summary.errorMessages.mkString("; ")
      ))
      val summarySchema = StructType(Seq(
        StructField("job_id", StringType, nullable = false), StructField("job_name", StringType, nullable = true),
        StructField("overall_status", StringType, nullable = true), StructField("start_time_ms", LongType, nullable = true),
        StructField("end_time_ms", LongType, nullable = true), StructField("duration_ms", LongType, nullable = true),
        StructField("source_name", StringType, nullable = true), StructField("target_name", StringType, nullable = true),
        StructField("rc_status", StringType, nullable = true), StructField("rc_source_row_count", LongType, nullable = true),
        StructField("rc_target_row_count", LongType, nullable = true), StructField("rc_difference", LongType, nullable = true),
        StructField("schema_status", StringType, nullable = true), StructField("schema_mismatch_count", IntegerType, nullable = true),
        StructField("dm_status", StringType, nullable = true), StructField("dm_source_total_keys", LongType, nullable = true),
        StructField("dm_target_total_keys", LongType, nullable = true), StructField("dm_matched_key_count", LongType, nullable = true),
        StructField("dm_source_only_key_count", LongType, nullable = true), StructField("dm_target_only_key_count", LongType, nullable = true),
        StructField("vc_status", StringType, nullable = true), StructField("vc_total_compared_rows", LongType, nullable = true),
        StructField("vc_mismatched_row_count", LongType, nullable = true), StructField("br_status", StringType, nullable = true),
        StructField("br_total_rules", IntegerType, nullable = true), StructField("br_failed_rules", IntegerType, nullable = true),
        StructField("error_messages_summary", StringType, nullable = true)
      ))
      val comprehensiveSummaryDf = spark.createDataFrame(spark.sparkContext.parallelize(summaryData), summarySchema)
      val summaryTableName = s"${hiveOutputConfig.databaseName}.${hiveOutputConfig.summaryTableName}"
      logger.info(s"Saving comprehensive reconciliation summary to Hive table: $summaryTableName")
      comprehensiveSummaryDf.write.mode(SaveMode.Append).format("hive").saveAsTable(summaryTableName)

      // 2. Save Detailed Events (New)
      saveReconDetailsToHive(summary, jobConfig, hiveOutputConfig, mismatchesDfFromValueComp, sourceOnlyRecordsDf, targetOnlyRecordsDf)

      // 3. Save Mismatch Details (Original table - consider for deprecation)
      // This table might be redundant if all value mismatches go into recon_details.
      // For now, keeping it if configured and `mismatchTableName` is different from `detailTableName`.
      if (hiveOutputConfig.mismatchTableName != hiveOutputConfig.detailTableName.getOrElse("")) {
          mismatchesDfFromValueComp.foreach { df =>
            val mismatchDetailTableName = s"${hiveOutputConfig.databaseName}.${hiveOutputConfig.mismatchTableName}"
            logger.warn(s"Also saving raw value comparison mismatched records to separate Hive table: $mismatchDetailTableName. This might be deprecated in favor of the details table.")
            df.write.mode(SaveMode.Overwrite).format("hive").saveAsTable(mismatchDetailTableName)
          }
      }

      logger.info(s"Successfully saved outputs to Hive database: ${hiveOutputConfig.databaseName}")
    } match {
      case scala.util.Failure(ex) =>
        logger.error(s"Failed to save results to Hive: ${ex.getMessage}", ex)
      case _ => // Success
    }
  }
}
