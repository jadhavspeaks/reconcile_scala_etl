package com.example.reco.readers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Reads CSV files into a DataFrame.
 */
class CsvReader extends DataReader {

  /**
   * Reads a CSV file from the given path.
   *
   * @param path Path to the CSV file or directory.
   * @param options A map of options for the CSV reader (e.g., "header", "delimiter", "inferSchema").
   * @param schema An optional schema to use for reading the CSV. If None, and "inferSchema" is not true,
   *               Spark will try to infer types, or a schema must be provided if headers are not present.
   * @param spark The implicit SparkSession.
   * @return DataFrame containing the CSV data.
   */
  override def read(path: String, options: Map[String, String] = Map.empty, schema: Option[StructType] = None)(implicit spark: SparkSession): DataFrame = {
    val reader = spark.read.options(options)

    schema match {
      case Some(s) => reader.schema(s).csv(path)
      case None    => reader.csv(path) // Relies on options like "inferSchema" or "header" being set appropriately
    }
  }
}

/**
 * Companion object for CsvReader.
 */
object CsvReader {
  // You can add factory methods or constants here if needed, for example:
  // def apply(): CsvReader = new CsvReader()
}
