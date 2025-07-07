package com.example.reco.readers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Trait for data readers.
 */
trait DataReader {
  /**
   * Reads data from a source.
   *
   * @param path Path to the data source.
   * @param options A map of options for the reader.
   * @param schema An optional schema to use for reading the data.
   * @return DataFrame containing the data.
   */
  def read(path: String, options: Map[String, String] = Map.empty, schema: Option[StructType] = None)(implicit spark: SparkSession): DataFrame
}

/**
 * A companion object for DataReader (if needed for utility functions or constants later)
 */
object DataReader {
  // Potential future utility methods or constants related to data reading
}
