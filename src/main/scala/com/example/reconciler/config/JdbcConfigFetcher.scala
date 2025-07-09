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

  // No longer using json4s for parsing the main config structure from JDBC.
  // implicit val formats: Formats = ... (removed)


  // --- Column Name Constants from Oracle ResultSet ---
  // These must match the column names in your RECON_JOBS_SQL_QUERY output
  // Simple direct map fields for ReconciliationJobConfig
  val JOB_ID_COL = "job_id"
  val JOB_NAME_COL = "job_name" // Also used in WHERE clause for filtering
  val PERFORM_ROW_COUNT_CHECK_COL = "perform_row_count_check" // String "true"/"false" or "yes"/"no"
  val PERFORM_SCHEMA_CHECK_COL = "perform_schema_check"       // String "true"/"false" or "yes"/"no"
  val PERFORM_DATA_RECON_COL = "perform_data_recon"         // String "true"/"false" or "yes"/"no"
  val CHECK_BUSINESS_TRANSFORMATION_COL = "check_business_transformation" // String "Yes"/"No"
  val SAMPLE_MISMATCH_LIMIT_COL = "sample_mismatch_limit"     // String representing an Int
  val ERROR_TOLERANCE_PERCENTAGE_COL = "error_tolerance_percentage" // String representing a Double
  val TIMEOUT_SECONDS_COL = "timeout_seconds"                 // String representing an Int

  // For SourceConfig (type discriminator and specific fields)
  val SOURCE_TYPE_COL = "source_type" // String: "file" or "hive"
  val SOURCE_FILE_PATH_COL = "source_file_path"
  val SOURCE_FILE_FORMAT_COL = "source_file_format" // String: "CSV", "EXCEL", etc. (matches FileFormat enum)
  val SOURCE_FILE_DELIMITER_COL = "source_file_delimiter"
  val SOURCE_FILE_HEADER_COL = "source_file_header" // String "true"/"false"
  // ... other FileSourceConfig fields (sheetName, multiLine, customSchemaJson, inferSchema) would need their own columns if not using JSON
  // For simplicity, assuming customSchema and inferSchema are not used for now, or are part of a simplified file config.
  val SOURCE_HIVE_DB_COL = "source_hive_db"
  val SOURCE_HIVE_TABLE_COL = "source_hive_table"

  // For TargetConfig (similar structure)
  val TARGET_TYPE_COL = "target_type"
  val TARGET_FILE_PATH_COL = "target_file_path"
  val TARGET_FILE_FORMAT_COL = "target_file_format"
  val TARGET_FILE_DELIMITER_COL = "target_file_delimiter"
  val TARGET_FILE_HEADER_COL = "target_file_header"
  val TARGET_HIVE_DB_COL = "target_hive_db"
  val TARGET_HIVE_TABLE_COL = "target_hive_table"

  // For Sequences (comma-separated strings)
  val PK_COLUMNS_STR_COL = "pk_columns_str" // e.g., "id,name"
  val COMPARE_COLUMN_NAMES_STR_COL = "compare_column_names_str" // e.g., "colA,colB,colC"

  // For Single Business Rule (if checkBusinessTransformation is "Yes")
  val BUSINESS_RULE_NAME_COL = "business_rule_name"
  val BUSINESS_RULE_SQL_COL = "business_rule_sql"
  val BUSINESS_RULE_EXPECTED_RESULT_COL = "business_rule_expected_result"

  // For HdfsOutputConfig (Option[HdfsOutputConfig]) - check existence by a key field like path
  val HDFS_OUTPUT_PATH_COL = "hdfs_output_path"
  val HDFS_OUTPUT_FORMAT_COL = "hdfs_output_format"

  // For HiveOutputConfig (Option[HiveOutputConfig]) - check by db_name
  val HIVE_OUTPUT_DB_NAME_COL = "hive_output_db_name"
  val HIVE_OUTPUT_SUMMARY_TABLE_COL = "hive_output_summary_table"
  val HIVE_OUTPUT_MISMATCH_TABLE_COL = "hive_output_mismatch_table"
  val HIVE_OUTPUT_DETAIL_TABLE_COL = "hive_output_detail_table" // Optional

  // For EmailConfig (Option[EmailConfig]) - check by enabled flag or smtp_host
  val EMAIL_ENABLED_COL = "email_enabled" // String "true"/"false"
  val EMAIL_RECIPIENTS_STR_COL = "email_recipients_str" // Comma-separated
  val EMAIL_SUBJECT_PREFIX_COL = "email_subject_prefix"
  val EMAIL_SMTP_HOST_COL = "email_smtp_host"
  val EMAIL_SMTP_PORT_COL = "email_smtp_port" // String representing Int
  val EMAIL_SMTP_USER_COL = "email_smtp_user"
  val EMAIL_SMTP_PASSWORD_COL = "email_smtp_password"
  val EMAIL_STARTTLS_ENABLED_COL = "email_starttls_enabled" // String "true"/"false"


  // Helper to safely parse boolean from string (Yes/No, True/False, case insensitive)
  private def safeParseBoolean(s: String, default: Boolean): Boolean = {
    if (s == null) default
    else {
      s.trim.toLowerCase match {
        case "true" | "yes" | "y" | "1" => true
        case "false" | "no" | "n" | "0" => false
        case _ => default
      }
    }
  }

  // Helper to safely parse Option[String] to Option[Int]
  private def safeParseOptionInt(optS: Option[String]): Option[Int] = optS.flatMap(s => Try(s.toInt).toOption)

  // Helper to safely parse Option[String] to Option[Double]
  private def safeParseOptionDouble(optS: Option[String]): Option[Double] = optS.flatMap(s => Try(s.toDouble).toOption)


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
          // --- Direct Mappings from ResultSet ---
          val jobId = rs.getString(JOB_ID_COL)
          val jobName = rs.getString(JOB_NAME_COL)

          // Boolean flags (assuming string "true"/"false" or "yes"/"no" from DB)
          val performRowCountCheck = safeParseBoolean(rs.getString(PERFORM_ROW_COUNT_CHECK_COL), default = true)
          val performSchemaCheck = safeParseBoolean(rs.getString(PERFORM_SCHEMA_CHECK_COL), default = true)
          val performDataReconciliation = safeParseBoolean(rs.getString(PERFORM_DATA_RECON_COL), default = true)
          val checkBusinessTransformation = safeParseBoolean(rs.getString(CHECK_BUSINESS_TRANSFORMATION_COL), default = false)

          // Int/Double fields (optional, from string columns)
          val sampleMismatchLimit = safeParseOptionInt(Option(rs.getString(SAMPLE_MISMATCH_LIMIT_COL))).getOrElse(100)
          val errorTolerancePercentage = safeParseOptionDouble(Option(rs.getString(ERROR_TOLERANCE_PERCENTAGE_COL)))
          val timeoutSeconds = safeParseOptionInt(Option(rs.getString(TIMEOUT_SECONDS_COL))).orElse(Some(60))


          // Source Config
          val sourceType = Option(rs.getString(SOURCE_TYPE_COL)).map(_.toLowerCase)
          val sourceConfig: DataSourceConfig = sourceType match {
            case Some("file") =>
              SourceFileConfig(FileSourceConfig(
                path = rs.getString(SOURCE_FILE_PATH_COL),
                format = FileFormat.withName(rs.getString(SOURCE_FILE_FORMAT_COL).toUpperCase),
                delimiter = Option(rs.getString(SOURCE_FILE_DELIMITER_COL)),
                header = Option(rs.getString(SOURCE_FILE_HEADER_COL)).map(s => safeParseBoolean(s, default=false))
                // TODO: Add other FileSourceConfig fields if they have corresponding flat columns:
                // sheetName: Option[String] = None,
                // multiLine: Option[Boolean] = None,
                // customSchema: Option[Seq[SchemaColumnConfig]]] = None, (this would be hard as flat)
                // inferSchema: Option[Boolean] = Some(false)
              ))
            case Some("hive") =>
              SourceHiveTableConfig(HiveSourceConfig(
                databaseName = rs.getString(SOURCE_HIVE_DB_COL),
                tableName = rs.getString(SOURCE_HIVE_TABLE_COL)
              ))
            case _ => throw new MappingException(s"Invalid or missing source_type: ${sourceType.getOrElse("NULL")}")
          }

          // Target Config (similar logic)
          val targetType = Option(rs.getString(TARGET_TYPE_COL)).map(_.toLowerCase)
          val targetConfig: DataSourceConfig = targetType match {
            case Some("file") =>
              SourceFileConfig(FileSourceConfig(
                path = rs.getString(TARGET_FILE_PATH_COL),
                format = FileFormat.withName(rs.getString(TARGET_FILE_FORMAT_COL).toUpperCase),
                delimiter = Option(rs.getString(TARGET_FILE_DELIMITER_COL)),
                header = Option(rs.getString(TARGET_FILE_HEADER_COL)).map(s => safeParseBoolean(s, default=false))
              ))
            case Some("hive") =>
              SourceHiveTableConfig(HiveSourceConfig(
                databaseName = rs.getString(TARGET_HIVE_DB_COL),
                tableName = rs.getString(TARGET_HIVE_TABLE_COL)
              ))
            case _ => throw new MappingException(s"Invalid or missing target_type: ${targetType.getOrElse("NULL")}")
          }

          // Primary Key Columns (comma-separated string)
          val primaryKeyColumns: Seq[String] = Option(rs.getString(PK_COLUMNS_STR_COL))
            .map(_.split(',').map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty)

          // Columns to Compare (comma-separated string of names - Assumption A: names only, use defaults)
          val columnsToCompare: Seq[ReconColumnConfig] = Option(rs.getString(COMPARE_COLUMN_NAMES_STR_COL))
            .map(_.split(',').map(_.trim).filter(_.nonEmpty)
              .map(name => ReconColumnConfig(
                columnName = name,
                isPrimaryKey = primaryKeyColumns.contains(name), // Auto-mark if it's also a PK
                tolerance = None, // Default
                ignoreCase = Some(false), // Default
                trimWhitespace = Some(true) // Default
              )).toSeq
            ).getOrElse(Seq.empty)

          // Business Rule (single rule, conditional)
          val businessRules: Option[Seq[BusinessRuleConfig]] = if (checkBusinessTransformation) {
            val ruleNameOpt = Option(rs.getString(BUSINESS_RULE_NAME_COL))
            val sqlOpt = Option(rs.getString(BUSINESS_RULE_SQL_COL))
            // Only create rule if name and SQL are present
            (ruleNameOpt, sqlOpt) match {
              case (Some(name), Some(sql)) if name.nonEmpty && sql.nonEmpty =>
                Some(Seq(BusinessRuleConfig(
                  ruleName = name,
                  sqlQuery = sql,
                  expectedResult = Option(rs.getString(BUSINESS_RULE_EXPECTED_RESULT_COL))
                )))
              case _ =>
                logger.warn(s"Business rule transformation checked for job '$jobName' but rule name or SQL is missing. Skipping business rule.")
                None
            }
          } else {
            None
          }

          // HDFS Output Config (Optional)
          val hdfsOutput: Option[HdfsOutputConfig] = Option(rs.getString(HDFS_OUTPUT_PATH_COL)).map { path =>
            HdfsOutputConfig(
              path = path,
              format = Option(rs.getString(HDFS_OUTPUT_FORMAT_COL)).getOrElse("parquet")
            )
          }

          // Hive Output Config (Optional)
          val hiveOutput: Option[HiveOutputConfig] = Option(rs.getString(HIVE_OUTPUT_DB_NAME_COL)).map { dbName =>
            HiveOutputConfig(
              databaseName = dbName,
              summaryTableName = rs.getString(HIVE_OUTPUT_SUMMARY_TABLE_COL),
              mismatchTableName = rs.getString(HIVE_OUTPUT_MISMATCH_TABLE_COL),
              detailTableName = Option(rs.getString(HIVE_OUTPUT_DETAIL_TABLE_COL))
            )
          }

          // Email Notifications (Optional)
          val emailEnabled = safeParseBoolean(rs.getString(EMAIL_ENABLED_COL), default = false)
          val emailNotifications: Option[EmailConfig] = if (emailEnabled && Option(rs.getString(EMAIL_SMTP_HOST_COL)).isDefined) {
            Some(EmailConfig(
              enabled = true,
              recipients = Option(rs.getString(EMAIL_RECIPIENTS_STR_COL)).map(_.split(',').map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty),
              subjectPrefix = Option(rs.getString(EMAIL_SUBJECT_PREFIX_COL)).getOrElse("[Recon Report]"),
              smtpHost = rs.getString(EMAIL_SMTP_HOST_COL),
              smtpPort = safeParseOptionInt(Option(rs.getString(EMAIL_SMTP_PORT_COL))).getOrElse(25),
              smtpUser = Option(rs.getString(EMAIL_SMTP_USER_COL)),
              smtpPassword = Option(rs.getString(EMAIL_SMTP_PASSWORD_COL)),
              starttlsEnabled = Option(rs.getString(EMAIL_STARTTLS_ENABLED_COL)).map(s => safeParseBoolean(s, default=true))
            ))
          } else {
            None
          }

          ReconciliationJobConfig(
            jobId = jobId,
            jobName = jobName,
            sourceConfig = sourceConfig,
            targetConfig = targetConfig,
            primaryKeyColumns = primaryKeyColumns,
            columnsToCompare = columnsToCompare,
            businessRules = businessRules,
            performRowCountCheck = performRowCountCheck,
            performSchemaCheck = performSchemaCheck,
            performDataReconciliation = performDataReconciliation,
            checkBusinessTransformation = checkBusinessTransformation, // Added this field
            hdfsOutput = hdfsOutput,
            hiveOutput = hiveOutput,
            emailNotifications = emailNotifications,
            sampleMismatchLimit = sampleMismatchLimit,
            errorTolerancePercentage = errorTolerancePercentage,
            timeoutSeconds = timeoutSeconds
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
