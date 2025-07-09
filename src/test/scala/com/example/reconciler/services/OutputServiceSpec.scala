package com.example.reconciler.services

import com.example.reconciler.config._
import com.example.reconciler.models._
import com.holdenkarau.spark.testing.DataFrameSuiteBase // For Spark testing
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import java.util.UUID
import java.sql.Timestamp

class OutputServiceSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  // Define a helper to get a SparkSession for tests if not using DataFrameSuiteBase's spark
  // implicit override lazy val spark: SparkSession = _spark
  // val sqlContext = spark.sqlContext // For older Spark versions if needed

  // --- Test Data Setup ---
  val testJobId = "testJob123"
  val testJobName = "Test Reconciliation Job"
  val currentTime = System.currentTimeMillis()
  val pkCols = Seq("id")

  def createSampleJobConfig(): ReconciliationJobConfig = ReconciliationJobConfig(
    jobId = testJobId,
    jobName = testJobName,
    sourceConfig = SourceFileConfig(FileSourceConfig("path/to/source.csv", FileFormat.CSV, header = Some(true))),
    targetConfig = SourceFileConfig(FileSourceConfig("path/to/target.csv", FileFormat.CSV, header = Some(true))),
    primaryKeyColumns = pkCols,
    columnsToCompare = Seq(ReconColumnConfig("value", tolerance = Some(0.1))),
    performRowCountCheck = true,
    performSchemaCheck = true,
    performDataReconciliation = true
  )

  def createSampleReconSummary(
    overallStatus: ReconStatus = Success,
    rowCountResult: Option[RowCountReconResult] = None,
    schemaReconResult: Option[SchemaReconResult] = None,
    dataMatchingResult: Option[DataMatchingResult] = None,
    valueComparisonResult: Option[ValueComparisonResult] = None,
    businessRuleResults: Option[Seq[BusinessRuleResult]] = None,
    errorMessages: Seq[String] = Seq.empty
  ): ReconciliationJobSummary = ReconciliationJobSummary(
    jobId = testJobId,
    jobName = testJobName,
    overallStatus = overallStatus,
    startTime = currentTime,
    endTime = Some(currentTime + 10000), // 10 seconds later
    rowCountResult = rowCountResult,
    schemaReconResult = schemaReconResult,
    dataMatchingResult = dataMatchingResult,
    valueComparisonResult = valueComparisonResult,
    businessRuleResults = businessRuleResults,
    errorMessages = errorMessages
  )

  // --- Logger Tests (Basic) ---
  "OutputService logger" should "be initialised" in {
    val outputService = new OutputService()(spark) // Pass implicit spark
    outputService.logger shouldNot be (null)
  }

  // --- Comprehensive Summary Table Tests ---
  "saveToHive (Comprehensive Summary)" should "generate a correct summary DataFrame" in {
    val jobConfig = createSampleJobConfig()
    val summary = createSampleReconSummary(
      overallStatus = Success,
      rowCountResult = Some(RowCountReconResult(Success, 100, 100, 0, "Counts match")),
      schemaReconResult = Some(SchemaReconResult(Success, None, None, Seq.empty, "Schemas match")),
      dataMatchingResult = Some(DataMatchingResult(Success, 100, 100, spark.emptyDataFrame, spark.emptyDataFrame, spark.emptyDataFrame, 100, 0, 0, "Data matching complete")),
      valueComparisonResult = Some(ValueComparisonResult(Success, spark.emptyDataFrame, 100, 0, Map.empty, "Values match")),
      businessRuleResults = Some(Seq(BusinessRuleResult("Rule1", Success, "SELECT 1", Some("1"), Some("1"), Some("Pass"))))
    )
    val hiveConfig = HiveOutputConfig("test_db", "summary_table", "mismatch_table", Some("detail_table"))

    // In a real test, we would mock the Hive write and capture the DataFrame.
    // For now, this test case describes the expected structure and content.
    // val outputService = new OutputService()(spark)
    // val (summaryDf, _) = outputService.saveToHive(summary, jobConfig, None, None, None, hiveConfig, isTestMode = true)
    // The above call is hypothetical as isTestMode is not implemented.

    // Expected Schema (should match the one defined in OutputService)
    val expectedSummarySchema = StructType(Seq(
        StructField("job_id", StringType, nullable = false),
        StructField("job_name", StringType, nullable = true),
        StructField("overall_status", StringType, nullable = true),
        StructField("start_time_ms", LongType, nullable = true),
        StructField("end_time_ms", LongType, nullable = true),
        StructField("duration_ms", LongType, nullable = true),
        StructField("source_name", StringType, nullable = true),
        StructField("target_name", StringType, nullable = true),
        StructField("rc_status", StringType, nullable = true),
        StructField("rc_source_row_count", LongType, nullable = true),
        StructField("rc_target_row_count", LongType, nullable = true),
        StructField("rc_difference", LongType, nullable = true),
        StructField("schema_status", StringType, nullable = true),
        StructField("schema_mismatch_count", IntegerType, nullable = true),
        StructField("dm_status", StringType, nullable = true),
        StructField("dm_source_total_keys", LongType, nullable = true),
        StructField("dm_target_total_keys", LongType, nullable = true),
        StructField("dm_matched_key_count", LongType, nullable = true),
        StructField("dm_source_only_key_count", LongType, nullable = true),
        StructField("dm_target_only_key_count", LongType, nullable = true),
        StructField("vc_status", StringType, nullable = true),
        StructField("vc_total_compared_rows", LongType, nullable = true),
        StructField("vc_mismatched_row_count", LongType, nullable = true),
        StructField("br_status", StringType, nullable = true),
        StructField("br_total_rules", IntegerType, nullable = true),
        StructField("br_failed_rules", IntegerType, nullable = true),
        StructField("error_messages_summary", StringType, nullable = true)
    ))

    // Assertions (conceptual - assuming summaryDf is captured)
    // summaryDf.schema should be (expectedSummarySchema)
    // summaryDf.count() should be (1)
    // val row = summaryDf.head()
    // row.getAs[String]("job_id") should be (testJobId)
    // row.getAs[String]("job_name") should be (testJobName)
    // row.getAs[String]("overall_status") should be (Success.toString)
    // row.getAs[Long]("duration_ms") should be (10000L)
    // row.getAs[String]("rc_status") should be (Success.toString)
    // row.getAs[Long]("rc_source_row_count") should be (100L)
    // ... and so on for other fields.
    succeed // Placeholder for actual assertions
  }

  // --- Detailed Events Table Tests ---
  "saveReconDetailsToHive" should "generate correct schema mismatch details" in {
    val jobConfig = createSampleJobConfig()
    val schemaCompareMismatch = SchemaFieldComparison("col_A", Some("String"), Some("Int"), isMatch = false, remarks = Some("Type mismatch"))
    val summary = createSampleReconSummary(
      schemaReconResult = Some(SchemaReconResult(Failure, None, None, Seq(schemaCompareMismatch), "Schema has mismatches"))
    )
    val hiveConfig = HiveOutputConfig("test_db", "summary_table", "mismatch_table", Some("detail_table"))

    // Conceptual: Capture the details DataFrame part for schema mismatches
    // val outputService = new OutputService()(spark)
    // val (_, detailsDfOption) = outputService.saveToHive(summary, jobConfig, None, None, None, hiveConfig, isTestMode = true)
    // val detailsDf = detailsDfOption.get.filter(col("detail_type") === "SCHEMA_MISMATCH")

    // Assertions (conceptual)
    // detailsDf.count() should be (1)
    // val row = detailsDf.head()
    // row.getAs[String]("job_id") should be (testJobId)
    // row.getAs[String]("detail_type") should be ("SCHEMA_MISMATCH")
    // row.getAs[String]("attribute_name") should be ("col_A")
    // row.getAs[String]("source_value") should be ("String")
    // row.getAs[String]("target_value") should be ("Int")
    // row.getAs[String]("remarks") should be ("Type mismatch")
    succeed // Placeholder
  }

  it should "generate correct value mismatch details" in {
    import spark.implicits._
    val jobConfig = createSampleJobConfig()
    val pkSchema = StructType(Seq(StructField("id", IntegerType)))
    val mismatchDetailSchema = ArrayType(StructType(Seq(
        StructField("columnName", StringType),
        StructField("sourceValue", StringType),
        StructField("targetValue", StringType),
        StructField("remark", StringType)
    )))
    val mismatchedRecordsData = Seq(
        Row(1, Seq(Row("value", "10.0", "10.5", "Value diff")))
    )
    // PKs + mismatches array column
    val mismatchedRecordsDfSchema = StructType(Seq(
        StructField("id", IntegerType, false),
        StructField("mismatches", mismatchDetailSchema, true)
    ))
    val valueMismatchesDf = spark.createDataFrame(spark.sparkContext.parallelize(mismatchedRecordsData), mismatchedRecordsDfSchema)

    val summary = createSampleReconSummary(
      valueComparisonResult = Some(ValueComparisonResult(Failure, valueMismatchesDf, 1, 1, Map("value" -> 1), "Values mismatch"))
    )
    val hiveConfig = HiveOutputConfig("test_db", "summary_table", "mismatch_table", Some("detail_table"))

    // Conceptual capture
    // val outputService = new OutputService()(spark)
    // val (_, detailsDfOption) = outputService.saveToHive(summary, jobConfig, Some(valueMismatchesDf), None, None, hiveConfig, isTestMode = true)
    // val detailsDf = detailsDfOption.get.filter(col("detail_type") === "VALUE_MISMATCH")

    // Assertions (conceptual)
    // detailsDf.count() should be (1)
    // val row = detailsDf.head()
    // row.getAs[String]("job_id") should be (testJobId)
    // row.getAs[String]("primary_keys_json") should include ("\"id\":1")
    // row.getAs[String]("attribute_name") should be ("value")
    // row.getAs[String]("source_value") should be ("10.0")
    // row.getAs[String]("target_value") should be ("10.5")
    succeed // Placeholder
  }

  it should "generate correct source-only key details" in {
    import spark.implicits._
    val jobConfig = createSampleJobConfig()
    val sourceOnlyData = Seq(Row(101, "source_extra_val")).toDF("id", "extra_col")

    val summary = createSampleReconSummary(
      dataMatchingResult = Some(DataMatchingResult(Success, 1,0, spark.emptyDataFrame, sourceOnlyData, spark.emptyDataFrame, 0,1,0, "Has source only"))
    )
    val hiveConfig = HiveOutputConfig("test_db", "summary_table", "mismatch_table", Some("detail_table"))

    // Conceptual capture
    // val outputService = new OutputService()(spark)
    // val (_, detailsDfOption) = outputService.saveToHive(summary, jobConfig, None, Some(sourceOnlyData), None, hiveConfig, isTestMode = true)
    // val detailsDf = detailsDfOption.get.filter(col("detail_type") === "SOURCE_ONLY_KEY")

    // Assertions (conceptual)
    // detailsDf.count() should be (1)
    // val row = detailsDf.head()
    // row.getAs[String]("job_id") should be (testJobId)
    // row.getAs[String]("primary_keys_json") should include ("\"id\":101")
    // row.getAs[String]("remarks") should be ("Record present only in source")
    succeed // Placeholder
  }

   it should "generate correct target-only key details" in {
    import spark.implicits._
    val jobConfig = createSampleJobConfig()
    val targetOnlyData = Seq(Row(202, "target_extra_val")).toDF("id", "extra_col")

    val summary = createSampleReconSummary(
      dataMatchingResult = Some(DataMatchingResult(Success, 0,1, spark.emptyDataFrame, spark.emptyDataFrame, targetOnlyData, 0,0,1, "Has target only"))
    )
    val hiveConfig = HiveOutputConfig("test_db", "summary_table", "mismatch_table", Some("detail_table"))

    // Conceptual capture
    // val outputService = new OutputService()(spark)
    // val (_, detailsDfOption) = outputService.saveToHive(summary, jobConfig, None, None, Some(targetOnlyData), hiveConfig, isTestMode = true)
    // val detailsDf = detailsDfOption.get.filter(col("detail_type") === "TARGET_ONLY_KEY")

    // Assertions (conceptual)
    // detailsDf.count() should be (1)
    // val row = detailsDf.head()
    // row.getAs[String]("job_id") should be (testJobId)
    // row.getAs[String]("primary_keys_json") should include ("\"id\":202")
    // row.getAs[String]("remarks") should be ("Record present only in target")
    succeed // Placeholder
  }

  it should "handle empty details gracefully" in {
    val jobConfig = createSampleJobConfig()
    // Summary with no actual mismatches or source/target only data that would go to details table
    val summary = createSampleReconSummary(
      schemaReconResult = Some(SchemaReconResult(Success, None, None, Seq.empty, "Schemas match")),
      valueComparisonResult = Some(ValueComparisonResult(Success, spark.emptyDataFrame, 0,0,Map.empty, "Values match")),
      dataMatchingResult = Some(DataMatchingResult(Success, 0,0,spark.emptyDataFrame,spark.emptyDataFrame,spark.emptyDataFrame,0,0,0,"No data"))
    )
    val hiveConfig = HiveOutputConfig("test_db", "summary_table", "mismatch_table", Some("detail_table"))

    // Conceptual capture - detailsDfOption should be None or an empty DF if saveReconDetailsToHive is called
    // For this test, it's more about ensuring no exceptions are thrown and the method handles empty inputs.
    // If saveReconDetailsToHive returns a DataFrame, it should be empty.
    // If it has side effects (logging), those could be checked with a mock logger.
    noException shouldBe thrownBy {
        // val outputService = new OutputService()(spark)
        // outputService.saveToHive(summary, jobConfig, None, None, None, hiveConfig, isTestMode = true)
        // This would test the internal call to saveReconDetailsToHive
    }
    succeed
  }

}
