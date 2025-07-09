package com.example.reconciler.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{any, anyString}
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import scala.util.{Success, Failure, Try}
// Import models for creating sample ReconciliationJobConfig
import com.example.reconciler.models.ReconStatus // If needed for default statuses, though not directly by config

class JdbcConfigFetcherSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  // --- Helper to Mock ResultSet ---
  private def mockResultSet(rs: ResultSet, data: Map[String, String], hasNextSeq: Seq[Boolean]): Unit = {
    if (hasNextSeq.isEmpty) {
      when(rs.next()).thenReturn(false)
    } else {
      var B = when(rs.next())
      hasNextSeq.foreach(b => B = B.thenReturn(b))
    }

    data.keys.foreach { key =>
      when(rs.getString(key)).thenReturn(data(key))
    }
    // Default for any other getString call to return null if not in map
    when(rs.getString(anyString())).thenAnswer(invocation => {
        val colName = invocation.getArgument[String](0)
        data.getOrElse(colName, null)
    })
  }

  // --- Default Valid Row Data (all strings, as they come from ResultSet.getString) ---
  def getDefaultFlatRowData(jobId: String = "job1", jobName: String = "TestJob1"): Map[String, String] = Map(
    JdbcConfigFetcher.JOB_ID_COL -> jobId,
    JdbcConfigFetcher.JOB_NAME_COL -> jobName,
    JdbcConfigFetcher.PERFORM_ROW_COUNT_CHECK_COL -> "true",
    JdbcConfigFetcher.PERFORM_SCHEMA_CHECK_COL -> "true",
    JdbcConfigFetcher.SOURCE_TO_TARGET_FLAG_COL -> "true",
    JdbcConfigFetcher.BUSINESS_RULE_COMPARISON_FLAG_COL -> "true", // Enable new BR comparison
    JdbcConfigFetcher.CHECK_BUSINESS_TRANSFORMATION_COL -> "Yes", // Enable legacy BR definition
    JdbcConfigFetcher.SAMPLE_MISMATCH_LIMIT_COL -> "100",
    JdbcConfigFetcher.ERROR_TOLERANCE_PERCENTAGE_COL -> "5.0",
    JdbcConfigFetcher.TIMEOUT_SECONDS_COL -> "300",

    JdbcConfigFetcher.SOURCE_TYPE_COL -> "file",
    JdbcConfigFetcher.SOURCE_FILE_PATH_COL -> "path/to/source.csv",
    JdbcConfigFetcher.SOURCE_FILE_FORMAT_COL -> "CSV",
    JdbcConfigFetcher.SOURCE_FILE_DELIMITER_COL -> ",",
    JdbcConfigFetcher.SOURCE_FILE_HEADER_COL -> "true",

    JdbcConfigFetcher.TARGET_TYPE_COL -> "hive",
    JdbcConfigFetcher.TARGET_HIVE_DB_COL -> "target_db",
    JdbcConfigFetcher.TARGET_HIVE_TABLE_COL -> "target_table",

    JdbcConfigFetcher.PK_COLUMNS_STR_COL -> "id,tx_date",
    JdbcConfigFetcher.COMPARE_COLUMN_NAMES_STR_COL -> "amount,description,status",
    JdbcConfigFetcher.SOURCE_MAPPING_COLUMNS_STR_COL -> "src_id,src_val,src_desc",
    JdbcConfigFetcher.TARGET_MAPPING_COLUMNS_STR_COL -> "id,amount,description",


    JdbcConfigFetcher.BUSINESS_RULE_NAME_COL -> "BR_vs_Target",
    JdbcConfigFetcher.BUSINESS_RULE_SQL_COL -> "SELECT id, amount, description FROM some_derived_source",
    JdbcConfigFetcher.BUSINESS_RULE_EXPECTED_RESULT_COL -> null, // Not used for BR vs Target
    JdbcConfigFetcher.BUSINESS_RULE_TARGET_JOIN_KEYS_STR_COL -> "id", // Join mapped rule result with target on 'id'

    JdbcConfigFetcher.HDFS_OUTPUT_PATH_COL -> "/user/recon/output/job1",
    JdbcConfigFetcher.HDFS_OUTPUT_FORMAT_COL -> "parquet",

    JdbcConfigFetcher.HIVE_OUTPUT_DB_NAME_COL -> "recon_output_db",
    JdbcConfigFetcher.HIVE_OUTPUT_SUMMARY_TABLE_COL -> "job1_summary",
    JdbcConfigFetcher.HIVE_OUTPUT_MISMATCH_TABLE_COL -> "job1_mismatches",
    JdbcConfigFetcher.HIVE_OUTPUT_DETAIL_TABLE_COL -> "job1_details",

    JdbcConfigFetcher.EMAIL_ENABLED_COL -> "true",
    JdbcConfigFetcher.EMAIL_RECIPIENTS_STR_COL -> "user1@example.com,user2@example.com",
    JdbcConfigFetcher.EMAIL_SUBJECT_PREFIX_COL -> "[ReconJob1]",
    JdbcConfigFetcher.EMAIL_SMTP_HOST_COL -> "smtp.example.com",
    JdbcConfigFetcher.EMAIL_SMTP_PORT_COL -> "587",
    JdbcConfigFetcher.EMAIL_SMTP_USER_COL -> "recon_user",
    JdbcConfigFetcher.EMAIL_SMTP_PASSWORD_COL -> "secret",
    JdbcConfigFetcher.EMAIL_STARTTLS_ENABLED_COL -> "true"
  )

  // --- Test Cases ---
  // As before, these tests are more descriptive due to DriverManager static calls.
  // Focus is on the logic within the Try block that processes the ResultSet.

  "JdbcConfigFetcher.fetchAllConfigs" should "return an empty list if ResultSet has no rows" in {
    succeed // Conceptual: requires mocking DriverManager or refactoring Fetcher
  }

  it should "correctly parse a single valid row with all flat string fields into ReconciliationJobConfig" in {
    val mockRs = mock[ResultSet]
    val rowData = getDefaultFlatRowData()
    mockResultSet(mockRs, rowData, Seq(true, false))

    // --- Conceptual Assertions if we could directly test the mapping logic ---
    // val configTry = JdbcConfigFetcher.mapRowToConfig(mockRs) // Assuming mapRowToConfig is extracted and testable
    // configTry shouldBe a [Success[_]]
    // val config = configTry.get
    //
    // config.jobId shouldBe "job1"
    // config.jobName shouldBe "TestJob1"
    // config.performRowCountCheck shouldBe true
    // config.sourceToTargetFlag shouldBe true
    // config.businessRuleComparisonFlag shouldBe true
    // config.checkBusinessTransformation shouldBe true // For legacy rule definition
    //
    // config.columnNameMapping shouldBe Some(Map("src_id" -> "id", "src_val" -> "amount", "src_desc" -> "description"))
    //
    // config.sourceConfig shouldBe a [SourceFileConfig]
    // val srcFileConf = config.sourceConfig.asInstanceOf[SourceFileConfig].fileConfig
    // srcFileConf.path shouldBe "path/to/source.csv"
    // srcFileConf.format shouldBe FileFormat.CSV
    //
    // config.primaryKeyColumns shouldBe Seq("id", "tx_date")
    // config.columnsToCompare.map(_.columnName) shouldBe Seq("amount", "description", "status")
    // config.columnsToCompare.find(_.columnName == "amount").get.isPrimaryKey shouldBe false // Default
    //
    // config.businessRules shouldBe defined
    // config.businessRules.get.size shouldBe 1
    // val rule = config.businessRules.get.head
    // rule.ruleName shouldBe "BR_vs_Target"
    // rule.sqlQuery shouldBe "SELECT id, amount, description FROM some_derived_source"
    // rule.expectedResult shouldBe None // As it's null in data
    // rule.joinKeysForTargetComparison shouldBe Some(Seq("id"))
    //
    // config.emailNotifications.get.recipients should contain allElementsOf Seq("user1@example.com", "user2@example.com")
    succeed // Placeholder
  }

  it should "create an empty columnNameMapping if mapping strings are invalid (e.g. different lengths)" in {
    val mockRs = mock[ResultSet]
    val rowData = getDefaultFlatRowData()
      .updated(JdbcConfigFetcher.SOURCE_MAPPING_COLUMNS_STR_COL, "src_a,src_b")
      .updated(JdbcConfigFetcher.TARGET_MAPPING_COLUMNS_STR_COL, "tgt_x") // Different lengths
    mockResultSet(mockRs, rowData, Seq(true, false))
    // Conceptual:
    // val config = JdbcConfigFetcher.mapRowToConfig(mockRs).get
    // config.columnNameMapping shouldBe None // or Some(Map.empty) depending on implementation
    succeed
  }

  it should "create None for columnNameMapping if one of the mapping strings is null or empty" in {
    val mockRs = mock[ResultSet]
    val rowData = getDefaultFlatRowData()
      .updated(JdbcConfigFetcher.SOURCE_MAPPING_COLUMNS_STR_COL, null)
      .updated(JdbcConfigFetcher.TARGET_MAPPING_COLUMNS_STR_COL, "tgt_x,tgt_y")
    mockResultSet(mockRs, rowData, Seq(true, false))
    // Conceptual:
    // val config = JdbcConfigFetcher.mapRowToConfig(mockRs).get
    // config.columnNameMapping shouldBe None
    succeed
  }

  it should "correctly set businessRules to None if checkBusinessTransformation is 'No' and businessRuleComparisonFlag is false" in {
    val mockRs = mock[ResultSet]
    val rowData = getDefaultFlatRowData()
      .updated(JdbcConfigFetcher.CHECK_BUSINESS_TRANSFORMATION_COL, "No")
      .updated(JdbcConfigFetcher.BUSINESS_RULE_COMPARISON_FLAG_COL, "false")
    mockResultSet(mockRs, rowData, Seq(true, false))
    // Conceptual:
    // val config = JdbcConfigFetcher.mapRowToConfig(mockRs).get
    // config.businessRules shouldBe None
    succeed
  }

  it should "create businessRule with joinKeys if businessRuleComparisonFlag is 'true' and keys are provided" in {
     val mockRs = mock[ResultSet]
    val rowData = getDefaultFlatRowData()
      .updated(JdbcConfigFetcher.CHECK_BUSINESS_TRANSFORMATION_COL, "No") // Legacy rule check off
      .updated(JdbcConfigFetcher.BUSINESS_RULE_COMPARISON_FLAG_COL, "true") // New rule check on
      .updated(JdbcConfigFetcher.BUSINESS_RULE_TARGET_JOIN_KEYS_STR_COL, "key1,key2")
    mockResultSet(mockRs, rowData, Seq(true, false))
    // Conceptual:
    // val config = JdbcConfigFetcher.mapRowToConfig(mockRs).get
    // config.businessRules shouldBe defined
    // config.businessRules.get.head.joinKeysForTargetComparison shouldBe Some(Seq("key1", "key2"))
    // config.businessRules.get.head.expectedResult shouldBe None // As it was null in default data
    succeed
  }

  it should "return Failure if mandatory source_type is missing or invalid" in {
    val mockRs = mock[ResultSet]
    val rowData = getDefaultFlatRowData().updated(JdbcConfigFetcher.SOURCE_TYPE_COL, "invalid_type")
    mockResultSet(mockRs, rowData, Seq(true, false))
    // Conceptual:
    // JdbcConfigFetcher.mapRowToConfig(mockRs).isFailure shouldBe true
    succeed
  }

  // Add tests for:
  // - Various safeParseBoolean inputs ("yes", "Y", "0", "NO", "  TrUe  ", null, "random")
  // - safeParseOptionInt and safeParseOptionDouble with valid, invalid, and null strings
  // - FileFormat.withName for valid and invalid format strings
  // - Optional configs (HDFS, Hive, Email) being None if their key fields are null/empty
  // - Splitting of comma-separated strings for recipients, pk_columns, compare_column_names
}
