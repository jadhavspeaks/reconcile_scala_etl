package com.example.reconciler

import com.example.reconciler.config.{JdbcConfigFetcher, ReconciliationJobConfig} // Changed OracleConfigFetcher to JdbcConfigFetcher
import com.example.reconciler.readers.DataSourceReaderFactory
import com.example.reconciler.services.{ReconciliationService, OutputService, EmailService}
import com.example.reconciler.models._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.util.{Success => TrySuccess, Failure => TryFailure} // For Try matching

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

    // jobIdArg might be a specific job name to filter by, or None if all jobs from query are to be run.
    // If args is empty, jobIdArg becomes "sampleReconJob1", which we can treat as a filter.
    // If args has a value, that's the filter.
    // If RECON_JOBS_SQL_QUERY is general and returns multiple, and jobIdArg is not a specific filter, all will run.
    val jobNameFilter: Option[String] = if (args.isEmpty && jobIdArg == "sampleReconJob1") {
      // If default "sampleReconJob1" is used because no args, and SQL is general, maybe run all.
      // However, for now, let's assume jobIdArg always means a filter if provided.
      // If you want to run ALL jobs from the DB when no arg is given, jobNameFilter should be None.
      // This depends on the desired behavior of an empty `args`.
      // For now, if jobIdArg has a value (even default), use it as a filter.
      // To run all jobs, the user would need to modify the calling script or this logic.
      // A more explicit way: pass a special arg like "--all-jobs" or rely on RECON_JOBS_SQL_QUERY to not have a WHERE clause.
      // For now, simplifying: if jobIdArg is passed (even if it's the default "sampleReconJob1"), we filter by it.
      // To run for *all* jobs from the DB query, the user would need to ensure RECON_JOBS_SQL_QUERY fetches all
      // and potentially modify this Main to pass None to fetchAllConfigs if no specific job name is given.
      // Let's make it so if jobIdArg is provided, it's a filter. If no arg is provided, fetchAllConfigs gets None.
      args.headOption // This will be None if args is empty, Some(jobIdArg) otherwise.
    } else {
      args.headOption // Standard case: use the first argument as the filter.
    }


    var overallApplicationStatus = TrySuccess(()) // To track if any config parsing or job execution failed

    try {
      val fetchedConfigs: List[Try[ReconciliationJobConfig]] = JdbcConfigFetcher.fetchAllConfigs(jobNameFilter)

      if (fetchedConfigs.isEmpty) {
        if (jobNameFilter.isDefined) {
          logger.warn(s"No job configurations found for job name filter: ${jobNameFilter.get}. Exiting.")
        } else {
          logger.warn("No job configurations found from JDBC query. Exiting.")
        }
        overallApplicationStatus = TryFailure(new RuntimeException("No job configurations found."))
      }

      fetchedConfigs.foreach { configTry =>
        configTry match {
          case TrySuccess(jobConfig) =>
            logger.info(s"Successfully loaded configuration for job: ${jobConfig.jobName} (ID: ${jobConfig.jobId})")
            // --- Start of Single Job Execution Logic ---
            var jobSummary: Option[ReconciliationJobSummary] = Some(ReconciliationJobSummary(
              jobId = jobConfig.jobId,
              jobName = jobConfig.jobName,
              overallStatus = Success, // Initial status for this specific job
              startTime = Instant.now().toEpochMilli // Start time for this specific job
            ))
            val singleJobErrorMessages = ListBuffer[String]() // Errors specific to this job run

            try {
                logger.info(s"Executing reconciliation for job: ${jobConfig.jobName}")
                // ... (Keep the existing reconciliation logic here, from sourceReader down to email sending) ...
                // Replace 'appStartTime' with 'jobSummary.get.startTime' if needed for duration calcs within this job.

                val sourceReader = DataSourceReaderFactory.getReader(jobConfig.sourceConfig)
                val targetReader = DataSourceReaderFactory.getReader(jobConfig.targetConfig)

                logger.info(s"[${jobConfig.jobName}] Loading source DataFrame...")
                val originalSourceDf = sourceReader.read(jobConfig.sourceConfig)
                logger.info(s"[${jobConfig.jobName}] Source DataFrame loaded.")

                // Apply column mapping to sourceDf
                val sourceDf = jobConfig.columnNameMapping match {
                  case Some(mapping) if mapping.nonEmpty =>
                    logger.info(s"[${jobConfig.jobName}] Applying column mapping to source DataFrame.")
                    com.example.reconciler.util.DataFrameTransformer.applyColumnMapping(originalSourceDf, mapping)
                  case _ =>
                    logger.info(s"[${jobConfig.jobName}] No column mapping defined or mapping is empty. Using source DataFrame as is.")
                    originalSourceDf
                }

                logger.info(s"[${jobConfig.jobName}] Loading target DataFrame...")
                val targetDf = targetReader.read(jobConfig.targetConfig)
                logger.info(s"[${jobConfig.jobName}] Target DataFrame loaded.")

                val reconService = new ReconciliationService()
                var currentOverallStatus: ReconStatus = Success

                var finalRowCountResult: Option[RowCountReconResult] = None
                if (jobConfig.performRowCountCheck) { // This flag can remain for a general row count check
                  logger.info(s"[${jobConfig.jobName}] --- Performing Row Count Check ---")
                  // Row count is done on DFs before potential data type changes or detailed filtering in sourceToTargetFlag block
                  val rowCountResult = reconService.compareRowCounts(sourceDf, targetDf, jobConfig)
                  logger.info(s"[${jobConfig.jobName}] Row Count Result: ${rowCountResult.status} - ${rowCountResult.summaryMessage}")
                  if (rowCountResult.status == Failure) currentOverallStatus = Failure
                  finalRowCountResult = Some(rowCountResult)
                }

                var finalSchemaReconResult: Option[SchemaReconResult] = None
                var finalDataMatchingResult: Option[DataMatchingResult] = None
                var finalValueComparisonResult: Option[ValueComparisonResult] = None
                var sourceOnlyDfForOutput: Option[org.apache.spark.sql.DataFrame] = None
                var targetOnlyDfForOutput: Option[org.apache.spark.sql.DataFrame] = None
                var mismatchesDfForOutput: Option[org.apache.spark.sql.DataFrame] = None

                if (jobConfig.sourceToTargetFlag) {
                  logger.info(s"[${jobConfig.jobName}] --- Performing Source-to-Target Reconciliation ---")
                  if (jobConfig.performSchemaCheck) {
                    logger.info(s"[${jobConfig.jobName}] --- Performing Schema Check ---")
                    // Schema check uses the (potentially mapped) sourceDf
                    val schemaReconResult = reconService.compareSchemas(sourceDf, targetDf, jobConfig)
                    logger.info(s"[${jobConfig.jobName}] Schema Recon Result: ${schemaReconResult.status} - ${schemaReconResult.summaryMessage}")
                    schemaReconResult.fieldComparisons.filter(!_.isMatch).foreach { comp =>
                      logger.info(s"  [${jobConfig.jobName}] MISMATCH: Field: ${comp.fieldName}, Source: ${comp.sourceDataType.getOrElse("N/A")}, Target: ${comp.targetDataType.getOrElse("N/A")}, Remarks: ${comp.remarks.getOrElse("")}")
                    }
                    if (schemaReconResult.status == Failure) currentOverallStatus = Failure
                    finalSchemaReconResult = Some(schemaReconResult)
                  }

                  logger.info(s"[${jobConfig.jobName}] --- Performing Data Matching (Key-based) ---")
                  // Data matching uses the (potentially mapped) sourceDf
                  val dataMatchingResult = reconService.performDataMatching(sourceDf, targetDf, jobConfig)
                  finalDataMatchingResult = Some(dataMatchingResult)
                  sourceOnlyDfForOutput = Some(dataMatchingResult.sourceOnlyRecordsDf)
                  targetOnlyDfForOutput = Some(dataMatchingResult.targetOnlyRecordsDf)
                  logger.info(s"[${jobConfig.jobName}] ${dataMatchingResult.summaryMessage}")
                  if (dataMatchingResult.status == Failure) currentOverallStatus = Failure

                  if (dataMatchingResult.matchedKeyCount > 0) {
                    logger.info(s"[${jobConfig.jobName}] --- Performing Column Value Comparison ---")
                    // Value comparison uses the (potentially mapped) sourceDf's columns via matchedRecordsDf
                    val valueCompResult = reconService.compareColumnValues(dataMatchingResult.matchedRecordsDf, jobConfig)
                    finalValueComparisonResult = Some(valueCompResult)
                    mismatchesDfForOutput = Some(valueCompResult.mismatchedRecordsDf)
                    logger.info(s"[${jobConfig.jobName}] ${valueCompResult.summaryMessage}")
                    if (valueCompResult.status == Failure) currentOverallStatus = Failure
                  } else {
                    logger.info(s"[${jobConfig.jobName}] No matched keys found for Source-To-Target, skipping column value comparison.")
                  }
                } else {
                  logger.info(s"[${jobConfig.jobName}] Source-to-Target reconciliation skipped as per sourceToTargetFlag=false.")
                }

                // Legacy Business Rule Check (vs literal)
                var finalLegacyBusinessRuleResults: Option[Seq[BusinessRuleResult]] = None
                if (jobConfig.checkBusinessTransformation) { // This is the flag for the legacy rule
                    jobConfig.businessRules.filter(_.nonEmpty).foreach{ rules =>
                        // Filter for rules that are 'legacy' (have expectedResult or no target join keys)
                        val legacyRules = rules.filter(r => r.expectedResult.isDefined || r.joinKeysForTargetComparison.isEmpty)
                        if (legacyRules.nonEmpty) {
                            logger.info(s"[${jobConfig.jobName}] --- Executing Legacy Business Rules (vs literal) ---")
                            val bizRuleResults = reconService.executeBusinessRules(legacyRules) // Pass only legacy rules
                            finalLegacyBusinessRuleResults = Some(bizRuleResults)
                            bizRuleResults.foreach { brr =>
                                logger.info(s"[${jobConfig.jobName}] Legacy Rule: ${brr.ruleName}, Status: ${brr.status}, Expected: ${brr.expectedResult.getOrElse("N/A")}, Actual: ${brr.actualResult.getOrElse("N/A")}, Remarks: ${brr.remarks.getOrElse("")}")
                                if (brr.status == Failure) currentOverallStatus = Failure
                            }
                        }
                    }
                }

                // New Business Rule Comparison (SQL result vs Target)
                var finalBusinessRuleComparisonResult: Option[ValueComparisonResult] = None
                if (jobConfig.businessRuleComparisonFlag) {
                    jobConfig.businessRules.filter(_.nonEmpty).foreach { rules =>
                        // Filter for rules that are for new comparison mode (have target join keys)
                        val comparisonRules = rules.filter(r => r.joinKeysForTargetComparison.isDefined && r.joinKeysForTargetComparison.get.nonEmpty)
                        if (comparisonRules.nonEmpty) {
                             logger.info(s"[${jobConfig.jobName}] --- Executing Business Rule Comparison (SQL vs Target) ---")
                             // Assuming one such rule based on previous clarifications, taking the first.
                             comparisonRules.headOption.foreach { ruleToCompare =>
                                val brCompResult = reconService.executeAndCompareBusinessRule(ruleToCompare, targetDf, jobConfig)
                                finalBusinessRuleComparisonResult = brCompResult
                                brCompResult.foreach { res =>
                                    logger.info(s"[${jobConfig.jobName}] Business Rule Comparison ('${ruleToCompare.ruleName}' vs Target) Result: ${res.status} - ${res.summaryMessage}")
                                    if (res.status == Failure) currentOverallStatus = Failure
                                }
                             }
                        } else {
                            logger.info(s"[${jobConfig.jobName}] businessRuleComparisonFlag is true, but no suitable business rules with joinKeysForTargetComparison found.")
                        }
                    }
                }


                val jobEndTime = Instant.now().toEpochMilli
                val currentJobSummary = jobSummary.get.copy(
                    rowCountResult = finalRowCountResult,
                    schemaReconResult = finalSchemaReconResult, // From sourceToTargetFlag block
                    dataMatchingResult = finalDataMatchingResult, // From sourceToTargetFlag block
                    valueComparisonResult = finalValueComparisonResult, // From sourceToTargetFlag block
                    businessRuleResults = finalLegacyBusinessRuleResults, // Legacy rules
                    businessRuleComparisonResult = finalBusinessRuleComparisonResult, // New rule comparison result
                    overallStatus = currentOverallStatus,
                    endTime = Some(jobEndTime),
                    errorMessages = singleJobErrorMessages.toSeq // Use errors from this specific job
                )
                jobSummary = Some(currentJobSummary) // Update the jobSummary for this iteration

                logger.info(s"\n--- Overall Job Status for ${jobConfig.jobName}: ${currentJobSummary.overallStatus} ---")

                val outputService = new OutputService()
                jobConfig.hdfsOutput.foreach { hdfsConf =>
                  outputService.saveToHdfs(currentJobSummary, mismatchesDfForOutput, sourceOnlyDfForOutput, targetOnlyDfForOutput, hdfsConf)
                }
                jobConfig.hiveOutput.foreach { hiveConf =>
                  outputService.saveToHive(currentJobSummary, jobConfig, mismatchesDfForOutput, sourceOnlyDfForOutput, targetOnlyDfForOutput, hiveConf)
                }
                jobConfig.emailNotifications.filter(_.enabled).foreach { emailConf =>
                  val emailService = new EmailService()
                  emailService.sendReconReport(currentJobSummary, emailConf) match {
                    case scala.util.Success(_) => logger.info(s"[${jobConfig.jobName}] Email report dispatch initiated.")
                    case scala.util.Failure(e) =>
                      logger.error(s"[${jobConfig.jobName}] Failed to send email report: ${e.getMessage}", e)
                      // Optionally add this error to singleJobErrorMessages if it should be in the persisted summary
                  }
                }
                if (currentJobSummary.overallStatus == Failure) {
                    overallApplicationStatus = TryFailure(new RuntimeException(s"Job ${jobConfig.jobName} failed."))
                }

            } catch {
                case e: Exception =>
                    val jobSpecificFatalError = s"FATAL ERROR during reconciliation for job ${jobConfig.jobName} (ID: ${jobConfig.jobId}): ${e.getMessage}"
                    logger.error(jobSpecificFatalError, e)
                    singleJobErrorMessages += jobSpecificFatalError
                    jobSummary = jobSummary.map(_.copy( // jobSummary here is Option, map is safer
                        overallStatus = Failure,
                        errorMessages = singleJobErrorMessages.toSeq ++ Seq(s"Exception: ${e.toString}"),
                        endTime = Some(Instant.now().toEpochMilli)
                    ))
                    overallApplicationStatus = TryFailure(e) // Mark that at least one job had a fatal error
            }
            // --- End of Single Job Execution Logic ---

          case TryFailure(ex) =>
            logger.error(s"Failed to load or parse a job configuration: ${ex.getMessage}", ex)
            overallApplicationStatus = TryFailure(ex) // Mark that config parsing failed
        }
      }
    } catch {
      case e: Exception => // Catch exceptions during fetching all configs (e.g., DB down)
        logger.error(s"FATAL ERROR during initial configuration fetching or main processing loop: ${e.getMessage}", e)
        overallApplicationStatus = TryFailure(e)
    } finally {
      val appEndTimeTotal = Instant.now().toEpochMilli
      val totalDuration = appEndTimeTotal - appStartTime
      logger.info(s"All reconciliation processing finished in $totalDuration ms.")

      spark.stop()
      logger.info("Spark session stopped.")

      if (overallApplicationStatus.isFailure) {
        logger.error("Application finished with errors (either config parsing or one or more jobs failed).")
        System.exit(1)
      } else {
        logger.info("Application finished successfully.")
      }
    }
  }
}
