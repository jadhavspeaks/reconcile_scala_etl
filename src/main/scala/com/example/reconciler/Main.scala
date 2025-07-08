package com.example.reconciler

import com.example.reconciler.config.{OracleConfigFetcher, ReconciliationJobConfig}
import com.example.reconciler.readers.DataSourceReaderFactory
import com.example.reconciler.services.ReconciliationService
import com.example.reconciler.models._ // Wildcard import
import org.apache.spark.sql.SparkSession
import java.time.Instant
import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {
    val jobIdArg = args.headOption.getOrElse("sampleReconJob1") // Default to sample job ID if no arg provided
    val appStartTime = Instant.now().toEpochMilli

    println(s"Starting Reconciliation Job: $jobIdArg")

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(s"DataReconciler - $jobIdArg")
      .enableHiveSupport()
      // .master("local[*]") // Uncomment for local testing
      .getOrCreate()

    var jobSummary: Option[ReconciliationJobSummary] = None
    val errorMessages = ListBuffer[String]()

    try {
      val jobConfigOpt: Option[ReconciliationJobConfig] = OracleConfigFetcher.fetchConfig(jobIdArg)

      jobConfigOpt match {
        case Some(jobConfig) =>
          jobSummary = Some(ReconciliationJobSummary(
            jobId = jobConfig.jobId,
            jobName = jobConfig.jobName,
            overallStatus = Success, // Initial status
            startTime = appStartTime
          ))

          println(s"Successfully fetched configuration for job: ${jobConfig.jobName}")
          // println(s"Source: ${jobConfig.sourceConfig}") // Verbose
          // println(s"Target: ${jobConfig.targetConfig}") // Verbose

          val sourceReader = DataSourceReaderFactory.getReader(jobConfig.sourceConfig)
          val targetReader = DataSourceReaderFactory.getReader(jobConfig.targetConfig)

          println("Loading source DataFrame...")
          val sourceDf = sourceReader.read(jobConfig.sourceConfig)
          println("Source DataFrame loaded.")
          // sourceDf.printSchema()

          println("Loading target DataFrame...")
          val targetDf = targetReader.read(jobConfig.targetConfig)
          println("Target DataFrame loaded.")
          // targetDf.printSchema()

          val reconService = new ReconciliationService()
          var currentOverallStatus: ReconStatus = Success // CORRECTED THIS LINE

          var finalRowCountResult: Option[RowCountReconResult] = None
          if (jobConfig.performRowCountCheck) {
            println("\n--- Performing Row Count Check ---")
            val rowCountResult = reconService.compareRowCounts(sourceDf, targetDf, jobConfig)
            println(s"Row Count Result: ${rowCountResult.status}")
            println(rowCountResult.summaryMessage)
            if (rowCountResult.status == Failure) currentOverallStatus = Failure
            finalRowCountResult = Some(rowCountResult)
          }

          var finalSchemaReconResult: Option[SchemaReconResult] = None
          if (jobConfig.performSchemaCheck) {
            println("\n--- Performing Schema Check ---")
            val schemaReconResult = reconService.compareSchemas(sourceDf, targetDf, jobConfig)
            println(s"Schema Recon Result: ${schemaReconResult.status}")
            println(schemaReconResult.summaryMessage)
            schemaReconResult.fieldComparisons.filter(!_.isMatch).foreach { comp => // Print only mismatches
              println(s"  MISMATCH: Field: ${comp.fieldName}, Source: ${comp.sourceDataType.getOrElse("N/A")}, Target: ${comp.targetDataType.getOrElse("N/A")}, Remarks: ${comp.remarks.getOrElse("")}")
            }
            if (schemaReconResult.status == Failure) currentOverallStatus = Failure
            finalSchemaReconResult = Some(schemaReconResult)
          }

          if (jobConfig.performDataReconciliation) {
            println("\n--- Data Reconciliation (Placeholder) ---")
            // Placeholder: This will be implemented and its result will also affect currentOverallStatus
            // and be added to jobSummary
          }

          // Update jobSummary with results
          jobSummary = jobSummary.map(_.copy(
            rowCountResult = finalRowCountResult,
            schemaReconResult = finalSchemaReconResult,
            overallStatus = currentOverallStatus, // This might be updated again after data recon
            endTime = Some(Instant.now().toEpochMilli)
          ))

          println(s"\n--- Overall Job Status for ${jobConfig.jobName}: ${jobSummary.get.overallStatus} ---")
          jobSummary.foreach(s => println(s"Job Summary: $s"))


        case None =>
          val fatalErrorMsg = s"FATAL: Could not fetch or load configuration for job ID: $jobIdArg. Exiting."
          println(fatalErrorMsg)
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
        println(fatalErrorMsg)
        e.printStackTrace()
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

      println(s"Reconciliation Job $jobIdArg finished in $duration ms.")
      jobSummary.foreach { summary =>
        println("\n--- Final Reconciliation Job Summary ---")
        println(s"Job ID: ${summary.jobId}")
        println(s"Job Name: ${summary.jobName}")
        println(s"Overall Status: ${summary.overallStatus}")
        println(s"Start Time: ${summary.startTime}")
        println(s"End Time: ${summary.endTime.getOrElse("N/A")}")
        summary.rowCountResult.foreach { rcr =>
          println(s"Row Count Check: ${rcr.status} - ${rcr.summaryMessage}")
        }
        summary.schemaReconResult.foreach { sr =>
          println(s"Schema Check: ${sr.status} - ${sr.summaryMessage}")
          // Optionally print mismatch details again here if desired
        }
        if (summary.errorMessages.nonEmpty) {
          println("Errors:")
          summary.errorMessages.foreach(e => println(s"  - $e"))
        }
      }
      // Here, in a later step, we would pass `jobSummary` to an output module
      // e.g., OutputService.saveSummary(jobSummary.get)

      spark.stop()

      // Exit with non-zero status if the job failed
      if (jobSummary.exists(_.overallStatus == Failure)) {
        System.exit(1)
      }
    }
  }
}
