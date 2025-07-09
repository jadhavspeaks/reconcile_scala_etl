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
  expectedResult: Option[String], // NOTE: Not used by the new "SQL result vs Target table" comparison mode. Retained for potential other uses or legacy.
  joinKeysForTargetComparison: Option[Seq[String]] = None // Column names (after mapping) to join BusinessRule SQL result with Target table
)

case class HdfsOutputConfig(
  path: String,
  format: String = "parquet" // e.g., parquet, orc, csv
)

case class HiveOutputConfig(
  databaseName: String,
  summaryTableName: String,
  mismatchTableName: String, // This might be deprecated or reused if schema is compatible
  detailTableName: Option[String] = None // New table for all detailed events
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
  columnNameMapping: Option[Map[String, String]] = None, // Populated by JdbcConfigFetcher
  primaryKeyColumns: Seq[String], // Should refer to target column names (or source if no mapping for them)
  columnsToCompare: Seq[ReconColumnConfig], // Should refer to target column names (or source if no mapping for them)
  businessRules: Option[Seq[BusinessRuleConfig]] = None,
  performRowCountCheck: Boolean = true, // General flag
  performSchemaCheck: Boolean = true,   // General flag
  // Specific flags for major recon modes
  sourceToTargetFlag: Boolean = true, // Controls Source-vs-Target data/value reconciliation
  businessRuleComparisonFlag: Boolean = false, // Controls BusinessRuleSQL-vs-Target reconciliation
  checkBusinessTransformation: Boolean = false, // Controls if the single business rule (name,sql,expected) is loaded/used
  hdfsOutput: Option[HdfsOutputConfig] = None,
  hiveOutput: Option[HiveOutputConfig] = None,
  emailNotifications: Option[EmailConfig] = None,
  // Advanced options
  sampleMismatchLimit: Int = 100,
  errorTolerancePercentage: Option[Double] = None,
  timeoutSeconds: Option[Int] = Some(60)
)

object OracleConfigFetcher { // This object will be replaced by JdbcConfigFetcher logic elsewhere
  import org.json4s._
  import org.json4s.native.JsonMethods._
  import org.json4s.ext.EnumNameSerializer
  import org.slf4j.LoggerFactory // Added for logging
  import scala.util.{Try, Success => TrySuccess, Failure => TryFailure}

  private val logger = LoggerFactory.getLogger(OracleConfigFetcher.getClass)

  // Define a custom serializer for the DataSourceConfig sealed trait
  // This tells json4s how to distinguish between SourceFileConfig and SourceHiveTableConfig
  // based on a type hint field (e.g., "sourceType" or by structure if unambiguous)
  // For simplicity, we'll rely on json4s's default behavior for case classes,
  // but for sealed traits with non-obvious distinctions in JSON, a custom serializer or hints are needed.
  // Let's assume the JSON will have a field that distinguishes, or the structure is distinct enough.
  // A common way is to add a "type" field in the JSON.
  // If JSON structure for sourceConfig is like:
  // { "fileConfig": { ... } } OR { "hiveConfig": { ... } }
  // json4s can often handle this. Or we use type hints.

  implicit val formats: Formats = DefaultFormats +
    new EnumNameSerializer(FileFormat) +
    FieldSerializer[SourceFileConfig]() +
    FieldSerializer[SourceHiveTableConfig]() // Reverted to FieldSerializer


  /**
   * Fetches the reconciliation job configuration from an API endpoint.
   *
   * @param reconJobId The ID of the reconciliation job to fetch.
   * @param apiBaseUrl The base URL for the configuration API.
   * @return Option[ReconciliationJobConfig]
   */
  def fetchConfig(reconJobId: String, apiBaseUrl: String): Option[ReconciliationJobConfig] = {
    val apiUrl = s"$apiBaseUrl/$reconJobId"
    val apiKeyFromEnv = sys.env.get("RECON_API_KEY")
    val apiKey = apiKeyFromEnv.getOrElse {
      logger.warn("RECON_API_KEY environment variable not set. Using dummy API key.")
      "dummy-key-value"
    }
    val timeoutMillis = 30000 // 30 seconds connect and read timeout

    logger.info(s"Attempting to fetch configuration for job ID: $reconJobId from $apiUrl (using API key from env: ${apiKeyFromEnv.isDefined})")

    Try {
      val response = requests.get(
        apiUrl,
        headers = Map("X-API-Key" -> apiKey, "Accept" -> "application/json"),
        connectTimeout = timeoutMillis,
        readTimeout = timeoutMillis
      )

      if (response.statusCode == 200) {
        val jsonString = response.text()
        logger.debug(s"Received JSON response for job $reconJobId: $jsonString")
        parse(jsonString).extract[ReconciliationJobConfig]
      } else {
        val errorMsg = s"Failed to fetch config for $reconJobId. Status: ${response.statusCode}, Body: ${response.text()}"
        logger.error(errorMsg)
        throw new RuntimeException(s"API request failed with status ${response.statusCode}")
      }
    } match {
      case TrySuccess(config) => Some(config)
      case TryFailure(ex: requests.RequestsException) =>
        logger.error(s"HTTP request to API failed for job $reconJobId: ${ex.getMessage}", ex)
        None
      case TryFailure(ex: org.json4s.MappingException) =>
        logger.error(s"Failed to parse JSON configuration for job $reconJobId: ${ex.getMessage}", ex)
        None
      case TryFailure(ex) =>
        logger.error(s"An unexpected error occurred while fetching/parsing config for job $reconJobId: ${ex.getMessage}", ex)
        None
    }
  }

  // Sample JSON generation for testing (if API is not available)
  def generateSampleJsonForJob(jobId: String): String = {
    implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(FileFormat) + FieldSerializer[SourceFileConfig]() + FieldSerializer[SourceHiveTableConfig]()
    import org.json4s.native.Serialization.writePretty

    val sampleConfig = ReconciliationJobConfig(
      jobId = jobId,
      jobName = s"Sample $jobId Type Reconciliation",
      sourceConfig = SourceFileConfig(FileSourceConfig(
        path = s"hdfs:///user/data/input/$jobId.csv",
        format = FileFormat.CSV,
        delimiter = Some(","),
        header = Some(true),
        inferSchema = Some(true),
        customSchema = Some(Seq(
          SchemaColumnConfig("id", "IntegerType", nullable = false),
          SchemaColumnConfig("name", "StringType"),
          SchemaColumnConfig("value", "DoubleType", format = Some("0.00")),
          SchemaColumnConfig("event_date", "DateType", format = Some("yyyy-MM-dd"))
        ))
      )),
      targetConfig = SourceHiveTableConfig(HiveSourceConfig(
        databaseName = "raw_db",
        tableName = s"${jobId}_target_table"
      )),
      primaryKeyColumns = Seq("id"),
      columnsToCompare = Seq(
        ReconColumnConfig("id", isPrimaryKey = true),
        ReconColumnConfig("name", ignoreCase = Some(true), trimWhitespace = Some(true)),
        ReconColumnConfig("value", tolerance = Some(0.001)),
        ReconColumnConfig("event_date")
      ),
      hiveOutput = Some(HiveOutputConfig(
        databaseName = "recon_reports_db",
        summaryTableName = "job_summary",
        mismatchTableName = "job_mismatches_raw", // Could be deprecated
        detailTableName = Some("job_details")
      )),
      emailNotifications = Some(EmailConfig(
        recipients = Seq("test@example.com"),
        smtpHost = "smtp.example.com",
        smtpPort = 587
      )),
      timeoutSeconds = Some(120)
    )
    writePretty(sampleConfig)
  }

  // Main for quick testing of JSON generation
  def main(args: Array[String]): Unit = {
    println(generateSampleJsonForJob("sampleReconJob1"))
  }
}
