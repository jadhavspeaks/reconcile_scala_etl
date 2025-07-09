package com.example.reconciler.config

import com.example.reconciler.models.{ReconciliationJobSummary, ReconStatus, RowCountReconResult, SchemaReconResult, DataMatchingResult, ValueComparisonResult, BusinessRuleResult} // Required for ReconciliationJobConfig structure
import org.slf4j.LoggerFactory
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.util.{Try, Success, Failure}
import scala.collection.mutable.ListBuffer
// Import json4s necessities for later steps, even if not fully used in this initial step
import org.json4s._
import org.json4s.native.JsonMethods._

object JdbcConfigFetcher {
  private val logger = LoggerFactory.getLogger(getClass)

  // Define implicit formats for json4s parsing (will be used in Step 4)
  // This needs to be compatible with how ReconciliationJobConfig and its children are structured
  // For now, using DefaultFormats; may need EnumNameSerializer for FileFormat if it's part of JSON
  // and FieldSerializer for DataSourceConfig variants if they are directly in JSON.
  implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(FileFormat) +
    FieldSerializer[SourceFileConfig]() + FieldSerializer[SourceHiveTableConfig]()


  // Placeholder column names - replace with actual column names from your ResultSet
  // Simple direct map fields
  val JOB_ID_COL = "job_id"
  val JOB_NAME_COL = "job_name"
  val PERFORM_ROW_COUNT_CHECK_COL = "perform_row_count_check" // Boolean
  val PERFORM_SCHEMA_CHECK_COL = "perform_schema_check"       // Boolean
  val PERFORM_DATA_RECON_COL = "perform_data_recon"         // Boolean
  val SAMPLE_MISMATCH_LIMIT_COL = "sample_mismatch_limit"     // Int
  // Potentially more simple fields from ReconciliationJobConfig

  // Columns expected to contain JSON strings for complex/nested objects/sequences
  val SOURCE_CONFIG_JSON_COL = "source_config_json"
  val TARGET_CONFIG_JSON_COL = "target_config_json"
  val PK_COLUMNS_JSON_COL = "pk_columns_json" // Expected: JSON array of strings
  val COLUMNS_TO_COMPARE_JSON_COL = "columns_to_compare_json" // Expected: JSON array of ReconColumnConfig
  val BUSINESS_RULES_JSON_COL = "business_rules_json" // Expected: JSON array of BusinessRuleConfig
  val HDFS_OUTPUT_JSON_COL = "hdfs_output_json" // Expected: JSON object for HdfsOutputConfig
  val HIVE_OUTPUT_JSON_COL = "hive_output_json" // Expected: JSON object for HiveOutputConfig
  val EMAIL_NOTIFICATIONS_JSON_COL = "email_notifications_json" // Expected: JSON object for EmailConfig


  def fetchAllConfigs(jobNameFilter: Option[String]): List[Try[ReconciliationJobConfig]] = {
    val jdbcUrl = sys.env.getOrElse("RECON_JOBS_JDBC_URL", {
      logger.error("RECON_JOBS_JDBC_URL environment variable not set.")
      throw new IllegalStateException("RECON_JOBS_JDBC_URL not configured.")
    })
    val jdbcUser = sys.env.getOrElse("RECON_JOBS_JDBC_USER", {
      logger.error("RECON_JOBS_JDBC_USER environment variable not set.")
      throw new IllegalStateException("RECON_JOBS_JDBC_USER not configured.")
    })
    val jdbcPassword = sys.env.getOrElse("RECON_JOBS_JDBC_PASSWORD", {
      logger.error("RECON_JOBS_JDBC_PASSWORD environment variable not set.")
      throw new IllegalStateException("RECON_JOBS_JDBC_PASSWORD not configured.")
    })
    val jdbcDriver = sys.env.getOrElse("RECON_JOBS_JDBC_DRIVER", "oracle.jdbc.driver.OracleDriver")
    var baseQuery = sys.env.getOrElse("RECON_JOBS_SQL_QUERY", {
      logger.error("RECON_JOBS_SQL_QUERY environment variable not set.")
      throw new IllegalStateException("RECON_JOBS_SQL_QUERY not configured.")
    })

    var conn: Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    val configs = ListBuffer[Try[ReconciliationJobConfig]]()

    try {
      Class.forName(jdbcDriver)
      conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)

      val finalQuery = jobNameFilter match {
        case Some(name) =>
          // Ensure the base query doesn't have a WHERE clause already, or handle it more robustly.
          // Simple append for now. A more robust way might be to check if "WHERE" exists and append "AND" or "WHERE".
          if (baseQuery.toLowerCase.contains("where")) {
             s"$baseQuery AND $JOB_NAME_COL = ?"
          } else {
             s"$baseQuery WHERE $JOB_NAME_COL = ?"
          }
        case None => baseQuery
      }
      logger.info(s"Executing JDBC config query: $finalQuery")
      pstmt = conn.prepareStatement(finalQuery)
      jobNameFilter.foreach(name => pstmt.setString(1, name))

      rs = pstmt.executeQuery()

      while (rs.next()) {
        configs += Try {
          // --- Simple Direct Mappings (Examples) ---
          val jobId = rs.getString(JOB_ID_COL)
          val jobName = rs.getString(JOB_NAME_COL)
          val performRowCountCheck = Try(rs.getBoolean(PERFORM_ROW_COUNT_CHECK_COL)).getOrElse(true) // Default to true if column missing/null or error
          val performSchemaCheck = Try(rs.getBoolean(PERFORM_SCHEMA_CHECK_COL)).getOrElse(true)
          val performDataReconciliation = Try(rs.getBoolean(PERFORM_DATA_RECON_COL)).getOrElse(true)
          val sampleMismatchLimit = Try(rs.getInt(SAMPLE_MISMATCH_LIMIT_COL)).getOrElse(100)

          // --- JSON Mappings ---
          def parseJsonString[T](jsonString: String)(implicit manifest: Manifest[T]): T = {
            if (jsonString == null || jsonString.trim.isEmpty) {
              throw new MappingException(s"Cannot parse null or empty JSON string for type ${manifest.runtimeClass.getSimpleName}")
            }
            parse(jsonString).extract[T]
          }

          def parseOptionalJsonString[T](jsonString: String)(implicit manifest: Manifest[T]): Option[T] = {
            if (jsonString == null || jsonString.trim.isEmpty) None
            else Some(parse(jsonString).extract[T])
          }

          val sourceConfig: DataSourceConfig = parseJsonString[DataSourceConfig](rs.getString(SOURCE_CONFIG_JSON_COL))
          val targetConfig: DataSourceConfig = parseJsonString[DataSourceConfig](rs.getString(TARGET_CONFIG_JSON_COL))

          val primaryKeyColumns: Seq[String] = parseJsonString[Seq[String]](rs.getString(PK_COLUMNS_JSON_COL))
          val columnsToCompare: Seq[ReconColumnConfig] = parseJsonString[Seq[ReconColumnConfig]](rs.getString(COLUMNS_TO_COMPARE_JSON_COL))

          val businessRules: Option[Seq[BusinessRuleConfig]] = parseOptionalJsonString[Seq[BusinessRuleConfig]](rs.getString(BUSINESS_RULES_JSON_COL))
          val hdfsOutput: Option[HdfsOutputConfig] = parseOptionalJsonString[HdfsOutputConfig](rs.getString(HDFS_OUTPUT_JSON_COL))
          val hiveOutput: Option[HiveOutputConfig] = parseOptionalJsonString[HiveOutputConfig](rs.getString(HIVE_OUTPUT_JSON_COL))
          val emailNotifications: Option[EmailConfig] = parseOptionalJsonString[EmailConfig](rs.getString(EMAIL_NOTIFICATIONS_JSON_COL))

          // Other simple fields like errorTolerancePercentage, timeoutSeconds would be mapped directly
          // Ensuring to handle nulls from ResultSet before trying to convert
          val errorTolerancePercentageString = Option(rs.getString("error_tolerance_percentage"))
          val errorTolerancePercentage = Try(errorTolerancePercentageString.toDouble).toOption

          val timeoutSecondsString = rs.getString("timeout_seconds") // Assuming column name
          val timeoutSeconds = Try(timeoutSecondsString.toInt).toOption


          ReconciliationJobConfig(
            jobId = jobId,
            jobName = jobName,
            sourceConfig = sourceConfig, // Placeholder
            targetConfig = targetConfig, // Placeholder
            primaryKeyColumns = primaryKeyColumns, // Placeholder
            columnsToCompare = columnsToCompare, // Placeholder
            businessRules = businessRules, // Placeholder
            performRowCountCheck = performRowCountCheck,
            performSchemaCheck = performSchemaCheck,
            performDataReconciliation = performDataReconciliation,
            hdfsOutput = hdfsOutput, // Placeholder
            hiveOutput = hiveOutput, // Placeholder
            emailNotifications = emailNotifications, // Placeholder
            sampleMismatchLimit = sampleMismatchLimit,
            errorTolerancePercentage = errorTolerancePercentage, // Example of direct mapping with Option
            timeoutSeconds = timeoutSeconds // Example of direct mapping with Option
          )
        }.recoverWith { case ex: Exception =>
          logger.error(s"Failed to parse a job configuration row from ResultSet for jobNameFilter '$jobNameFilter': ${ex.getMessage}", ex)
          Failure(ex) // Keep it as a Failure in the list
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"JDBC configuration fetching failed: ${e.getMessage}", e)
        // If the whole fetch operation fails, we return a list with a single Failure
        // or an empty list if no partial results were obtained.
        // For now, if connection or query setup fails, it will likely result in an empty 'configs' list or
        // an exception thrown before loop, which this catch block handles.
        // Consider if a single Failure representing the whole op failure should be returned.
        // Current logic: if this top-level try fails, configs might be empty or partially filled.
        // It's better to ensure that if this overall try fails, the list contains that failure.
        if (configs.isEmpty) {
            configs += Failure(new RuntimeException(s"JDBC configuration fetching failed entirely: ${e.getMessage}", e))
        } else {
            // Log that some rows might have been processed before this general failure
            logger.warn("JDBC fetching failed after processing some rows. Only successfully parsed rows (if any) before this error will be returned as Success.", e)
        }
    } finally {
      Try(if (rs != null) rs.close()).recover{ case e => logger.warn("Failed to close ResultSet", e)}
      Try(if (pstmt != null) pstmt.close()).recover{ case e => logger.warn("Failed to close PreparedStatement", e)}
      Try(if (conn != null) conn.close()).recover{ case e => logger.warn("Failed to close Connection", e)}
    }
    configs.toList
  }
}
