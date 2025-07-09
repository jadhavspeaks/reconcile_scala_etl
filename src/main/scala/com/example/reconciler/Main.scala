package com.example.reconciler

import com.example.reconciler.config.{OracleConfigFetcher, ReconciliationJobConfig}
import com.example.reconciler.readers.DataSourceReaderFactory
import com.example.reconciler.services.{ReconciliationService, OutputService, EmailService} // Added OutputService, EmailService
import com.example.reconciler.models._ // Wildcard import
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory // Added for logging
import java.time.Instant
import scala.collection.mutable.ListBuffer

object Main {
  private val logger = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    val jobIdArg = args.headOption.getOrElse("sampleReconJob1") // Default to sample job ID if no arg provided
    val appStartTime = Instant.now().toEpochMilli
    logger.info(s"Starting Reconciliation Job: $jobIdArg")

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(s"DataReconciler - $jobIdArg")
      .enableHiveSupport()
      // .master("local[*]") // Uncomment for local testing
      .getOrCreate()

    var jobSummary: Option[ReconciliationJobSummary] = None
    val errorMessages = ListBuffer[String]()

    try {
      // Determine API Base URL: Spark conf -> CLI arg -> Default
      val defaultApiBaseUrl = "http://your-api-server.com/api/recon-configs" // Placeholder
      val apiBaseUrlFromSparkConf = spark.conf.getOption("spark.reconciler.apiBaseUrl")
      val apiBaseUrlFromCli = if (args.length > 1) Some(args(1)) else None

      val apiBaseUrl = apiBaseUrlFromSparkConf.orElse(apiBaseUrlFromCli).getOrElse {
        logger.warn(s"API Base URL not found in Spark config (spark.reconciler.apiBaseUrl) or CLI argument. Using default: $defaultApiBaseUrl")
        defaultApiBaseUrl
      }
      logger.info(s"Using API Base URL: $apiBaseUrl")

      val jobConfigOpt: Option[ReconciliationJobConfig] = OracleConfigFetcher.fetchConfig(jobIdArg, apiBaseUrl)

      jobConfigOpt match {
        case Some(jobConfig) =>
          jobSummary = Some(ReconciliationJobSummary(
            jobId = jobConfig.jobId,
            jobName = jobConfig.jobName,
            overallStatus = Success, // Initial status
            startTime = appStartTime
          ))

          logger.info(s"Successfully fetched configuration for job: ${jobConfig.jobName}")
          // logger.debug(s"Source: ${jobConfig.sourceConfig}") // Verbose
          // logger.debug(s"Target: ${jobConfig.targetConfig}") // Verbose

          val sourceReader = DataSourceReaderFactory.getReader(jobConfig.sourceConfig)
          val targetReader = DataSourceReaderFactory.getReader(jobConfig.targetConfig)

          logger.info("Loading source DataFrame...")
          val sourceDf = sourceReader.read(jobConfig.sourceConfig)
          logger.info("Source DataFrame loaded.")
          // sourceDf.printSchema() // Potentially log schema at DEBUG level if needed

          logger.info("Loading target DataFrame...")
          val targetDf = targetReader.read(jobConfig.targetConfig)
          logger.info("Target DataFrame loaded.")
          // targetDf.printSchema() // Potentially log schema at DEBUG level if needed

          val reconService = new ReconciliationService()
          var currentOverallStatus: ReconStatus = Success

          var finalRowCountResult: Option[RowCountReconResult] = None
          if (jobConfig.performRowCountCheck) {
            logger.info("\n--- Performing Row Count Check ---")
            val rowCountResult = reconService.compareRowCounts(sourceDf, targetDf, jobConfig)
            logger.info(s"Row Count Result: ${rowCountResult.status}")
            logger.info(rowCountResult.summaryMessage)
            if (rowCountResult.status == Failure) currentOverallStatus = Failure
            finalRowCountResult = Some(rowCountResult)
          }

          var finalSchemaReconResult: Option[SchemaReconResult] = None
          if (jobConfig.performSchemaCheck) {
            logger.info("\n--- Performing Schema Check ---")
            val schemaReconResult = reconService.compareSchemas(sourceDf, targetDf, jobConfig)
            logger.info(s"Schema Recon Result: ${schemaReconResult.status}")
            logger.info(schemaReconResult.summaryMessage)
            schemaReconResult.fieldComparisons.filter(!_.isMatch).foreach { comp =>
              logger.info(s"  MISMATCH: Field: ${comp.fieldName}, Source: ${comp.sourceDataType.getOrElse("N/A")}, Target: ${comp.targetDataType.getOrElse("N/A")}, Remarks: ${comp.remarks.getOrElse("")}")
            }
            if (schemaReconResult.status == Failure) currentOverallStatus = Failure
            finalSchemaReconResult = Some(schemaReconResult)
          }

          var finalDataMatchingResult: Option[DataMatchingResult] = None
          var finalValueComparisonResult: Option[ValueComparisonResult] = None
          var sourceOnlyDfForOutput: Option[org.apache.spark.sql.DataFrame] = None
          var targetOnlyDfForOutput: Option[org.apache.spark.sql.DataFrame] = None
          var mismatchesDfForOutput: Option[org.apache.spark.sql.DataFrame] = None

          if (jobConfig.performDataReconciliation) {
            logger.info("\n--- Performing Data Matching (Key-based) ---")
            val dataMatchingResult = reconService.performDataMatching(sourceDf, targetDf, jobConfig)
            finalDataMatchingResult = Some(dataMatchingResult)
            sourceOnlyDfForOutput = Some(dataMatchingResult.sourceOnlyRecordsDf)
            targetOnlyDfForOutput = Some(dataMatchingResult.targetOnlyRecordsDf)

            logger.info(dataMatchingResult.summaryMessage)
            if (dataMatchingResult.status == Failure) currentOverallStatus = Failure

            if (dataMatchingResult.matchedKeyCount > 0) {
              logger.info("\n--- Performing Column Value Comparison ---")
              val valueCompResult = reconService.compareColumnValues(dataMatchingResult.matchedRecordsDf, jobConfig)
              finalValueComparisonResult = Some(valueCompResult)
              mismatchesDfForOutput = Some(valueCompResult.mismatchedRecordsDf)
              logger.info(valueCompResult.summaryMessage)
              if (valueCompResult.status == Failure) currentOverallStatus = Failure
            } else {
              logger.info("No matched keys found, skipping column value comparison.")
            }
          }

          var finalBusinessRuleResults: Option[Seq[BusinessRuleResult]] = None
          jobConfig.businessRules.filter(_.nonEmpty).foreach { rules =>
            logger.info("\n--- Executing Business Rules ---")
            val bizRuleResults = reconService.executeBusinessRules(rules)
            finalBusinessRuleResults = Some(bizRuleResults)
            bizRuleResults.foreach { brr =>
              logger.info(s"Rule: ${brr.ruleName}, Status: ${brr.status}, Expected: ${brr.expectedResult.getOrElse("N/A")}, Actual: ${brr.actualResult.getOrElse("N/A")}, Remarks: ${brr.remarks.getOrElse("")}")
              if (brr.status == Failure) currentOverallStatus = Failure
            }
          }

          val jobEndTime = Instant.now().toEpochMilli
          jobSummary = jobSummary.map(_.copy(
            rowCountResult = finalRowCountResult,
            schemaReconResult = finalSchemaReconResult,
            dataMatchingResult = finalDataMatchingResult,
            valueComparisonResult = finalValueComparisonResult,
            businessRuleResults = finalBusinessRuleResults,
            overallStatus = currentOverallStatus,
            endTime = Some(jobEndTime)
          ))

          logger.info(s"\n--- Overall Job Status for ${jobConfig.jobName}: ${jobSummary.get.overallStatus} ---")
          // logger.debug(s"Job Summary (full object): ${jobSummary.get}") // Can be very verbose

          // 5. Output Results
          jobSummary.foreach { summary =>
            val outputService = new OutputService()
            jobConfig.hdfsOutput.foreach { hdfsConf =>
              outputService.saveToHdfs(summary, mismatchesDfForOutput, sourceOnlyDfForOutput, targetOnlyDfForOutput, hdfsConf)
            }
            jobConfig.hiveOutput.foreach { hiveConf =>
              outputService.saveToHive(summary, jobConfig, mismatchesDfForOutput, sourceOnlyDfForOutput, targetOnlyDfForOutput, hiveConf)
            }
            jobConfig.emailNotifications.filter(_.enabled).foreach { emailConf =>
               val emailService = new EmailService()
               emailService.sendReconReport(summary, emailConf) match {
                 case scala.util.Success(_) => logger.info("Email report dispatch initiated.")
                 case scala.util.Failure(e) => logger.error(s"Failed to send email report: ${e.getMessage}", e)
               }
            }
          }

        case None =>
          val fatalErrorMsg = s"FATAL: Could not fetch or load configuration for job ID: $jobIdArg. Exiting."
          logger.error(fatalErrorMsg)
          errorMessages += fatalErrorMsg
          jobSummary = Some(ReconciliationJobSummary( // Create a minimal summary for failure case
            jobId = jobIdArg,
            jobName = "Unknown (Config Fetch Failed)",
            overallStatus = Failure,
            startTime = appStartTime,
            endTime = Some(Instant.now().toEpochMilli),
            errorMessages = errorMessages.toSeq
          ))
          // System.exit(1) // Decided to let finally block handle logging
      }

    } catch {
      case e: Exception =>
        val fatalErrorMsg = s"FATAL ERROR during reconciliation process for job $jobIdArg: ${e.getMessage}"
        logger.error(fatalErrorMsg, e) // Pass exception for stack trace logging
        // e.printStackTrace() // Handled by logger
        errorMessages += fatalErrorMsg
        jobSummary = jobSummary.map(_.copy(
            overallStatus = Failure,
            errorMessages = errorMessages.toSeq ++ Seq(s"Exception: ${e.toString}"),
            endTime = Some(Instant.now().toEpochMilli)
          )).orElse(Some(ReconciliationJobSummary( // If jobSummary was None (e.g. error before config loaded)
            jobId = jobIdArg,
            jobName = jobSummary.map(_.jobName).getOrElse("Unknown (Exception Occurred)"),
            overallStatus = Failure,
            startTime = appStartTime,
            endTime = Some(Instant.now().toEpochMilli),
            errorMessages = errorMessages.toSeq ++ Seq(s"Exception: ${e.toString}")
          )))
        // System.exit(1)
    } finally {
      val appEndTime = Instant.now().toEpochMilli
      val duration = appEndTime - appStartTime

      // Update endTime if not already set (e.g. if error happened before summary was fully populated)
      jobSummary = jobSummary.map(s => if(s.endTime.isEmpty) s.copy(endTime = Some(appEndTime)) else s)

      logger.info(s"Reconciliation Job $jobIdArg finished in $duration ms.")
      jobSummary.foreach { summary =>
        logger.info("\n--- Final Reconciliation Job Summary ---")
        logger.info(s"Job ID: ${summary.jobId}")
        logger.info(s"Job Name: ${summary.jobName}")
        logger.info(s"Overall Status: ${summary.overallStatus}")
        logger.info(s"Start Time: ${summary.startTime}")
        logger.info(s"End Time: ${summary.endTime.getOrElse("N/A")}")
        summary.rowCountResult.foreach { rcr =>
          logger.info(s"Row Count Check: ${rcr.status} - ${rcr.summaryMessage}")
        }
        summary.schemaReconResult.foreach { sr =>
          logger.info(s"Schema Check: ${sr.status} - ${sr.summaryMessage}")
          // Optionally log mismatch details again here if desired at DEBUG level
        }
        if (summary.errorMessages.nonEmpty) {
          logger.info("Errors:")
          summary.errorMessages.foreach(e => logger.error(s"  - $e")) // Log errors with error level
        }
      }
      // OutputService handles saving summary via saveToHive

      spark.stop()
      logger.info("Spark session stopped.")

      // Exit with non-zero status if the job failed
      if (jobSummary.exists(_.overallStatus == Failure)) {
        System.exit(1)
      }
    }
  }
}
