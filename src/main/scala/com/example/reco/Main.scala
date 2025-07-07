package com.example.reco

import com.example.reco.config.ConfigLoader
import com.example.reco.readers.{CsvReader, HiveReader}
import com.example.reco.reporting.BasicReportGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Main {

  def main(args: Array[String]): Unit = {
    println("Starting Data Reconciliation Framework...")

    // --- 1. Initialize Spark Session ---
    implicit val spark: SparkSession = SparkSession.builder
      .appName("DataReconciliationFramework")
      .master("local[*]") // Use local master for now; configure for cluster in real deployment
      // .enableHiveSupport() // Enable Hive support if interacting with actual Hive tables
      .getOrCreate()

    println(s"Spark Session created. Version: ${spark.version}")

    // --- 2. Load Configuration (Stubbed) ---
    // In a real app, parse command-line args for config path
    val configPath = args.headOption.getOrElse("config/sample_job_config.json") // Default path
    val appConfigEither = ConfigLoader.load(configPath)

    appConfigEither match {
      case Left(errorMsg) =>
        println(s"Error loading configuration: $errorMsg")
        spark.stop()
        sys.exit(1)

      case Right(config) =>
        println(s"Successfully loaded configuration for job: ${config.jobName}")

        // --- 3. Initialize Services/Readers ---
        val csvReader = new CsvReader()
        val hiveReader = new HiveReader() // Assumes HiveContext is available via SparkSession with Hive support
        val reconService = new ReconciliationService()
        val reportGenerator = new BasicReportGenerator()

        // --- 4. Read Source A (e.g., CSV file) ---
        println(s"Reading Source A: ${config.sourceA.name}")
        val dfA: DataFrame = try {
          config.sourceA.path match {
            case Some(path) =>
              // Create a dummy DataFrame for Source A as we don't have sample_source.csv yet
              // This will be replaced by actual reading when test files are in place.
              println(s"Note: Creating dummy DataFrame for Source A ('${config.sourceA.name}') as file '$path' is not yet available.")
              import spark.implicits._
              Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
            // csvReader.read(path, config.sourceA.options, config.sourceA.schema)
            case None =>
              println(s"Error: Path not specified for file-based Source A: ${config.sourceA.name}")
              spark.emptyDataFrame
          }
        } catch {
          case e: Exception =>
            println(s"Error reading Source A (${config.sourceA.name}): ${e.getMessage}")
            spark.emptyDataFrame
        }
        println(s"Source A (${config.sourceA.name}) schema:")
        dfA.printSchema()
        dfA.show(5, truncate = false)

        // --- 5. Read Source B (e.g., Hive Table - Mocked for now) ---
        println(s"Reading Source B: ${config.sourceB.name}")
        val dfB: DataFrame = try {
          config.sourceB.tableName match {
            case Some(tableName) =>
              // This is a placeholder. Actual Hive reading requires a Hive environment and Hive support enabled in Spark.
              // For now, creating a dummy DataFrame.
              println(s"Note: Creating dummy DataFrame for Source B ('${config.sourceB.name}') as Hive table '$tableName' access is mocked.")
              import spark.implicits._
              Seq((1, "Alice", 100), (2, "Bob", 200), (4, "David", 400)).toDF("id", "name", "value")
            // hiveReader.read(tableName, config.sourceB.options, config.sourceB.schema)
            case None =>
              println(s"Error: Table name not specified for Hive-based Source B: ${config.sourceB.name}")
              spark.emptyDataFrame
          }
        } catch {
          case e: Exception =>
            // This catch block might be more relevant if trying to connect to a real Hive
            println(s"Error reading Source B (${config.sourceB.name}): ${e.getMessage}")
            spark.emptyDataFrame
        }
        println(s"Source B (${config.sourceB.name}) schema:")
        dfB.printSchema()
        dfB.show(5, truncate = false)

        // --- 6. Perform Row Count Reconciliation ---
        if (!dfA.isEmpty && !dfB.isEmpty) {
          println("\nPerforming Row Count Reconciliation...")
          val rowCountReconSummary = reconService.reconcileRowCount(dfA, dfB, config.sourceA.name, config.sourceB.name)
          reportGenerator.generateRowCountSummaryReport(rowCountReconSummary)
        } else {
          println("\nSkipping row count reconciliation due to errors reading one or both sources.")
        }

        // --- 7. Further Reconciliation Steps (Stubs for future) ---
        // e.g., Schema reconciliation, Data reconciliation on key columns, etc.
        println("\nFurther reconciliation steps (schema, data match) would go here.")


        // --- 8. Stop Spark Session ---
        println("\nData Reconciliation Framework finished.")
        spark.stop()
    }
  }
}
