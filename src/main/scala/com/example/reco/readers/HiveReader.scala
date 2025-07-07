package com.example.reco.readers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import scala.util.Try

/**
 * Reads data from a Hive table.
 * Assumes SparkSession is configured with Hive support.
 */
class HiveReader extends DataReader {

  /**
   * Reads a Hive table into a DataFrame.
   *
   * @param tableName The fully qualified name of the Hive table (e.g., "databaseName.tableName").
   *                  This parameter corresponds to the `path` parameter in the DataReader trait.
   * @param options A map of options. For Hive, these are generally not used for spark.read.table(),
   *                but included for trait compatibility. Could be used for query options if reading via SQL query.
   * @param schema An optional schema. For Hive tables, the schema is typically read from the Hive Metastore,
   *               so this parameter is usually ignored. Included for trait compatibility.
   * @param spark The implicit SparkSession.
   * @return DataFrame containing the Hive table data.
   * @throws org.apache.spark.sql.AnalysisException if the table is not found or other Hive access issues.
   */
  override def read(tableName: String, options: Map[String, String] = Map.empty, schema: Option[StructType] = None)(implicit spark: SparkSession): DataFrame = {
    if (schema.isDefined) {
      // Log a warning or info that schema is typically ignored for Hive reads as it's taken from Metastore
      println(s"Info: Schema provided for Hive table '$tableName' but will be ignored as schema is read from Metastore.")
    }
    if (options.nonEmpty && !options.contains("query")) { // Allow "query" option for SQL based reading
        println(s"Info: Options provided for Hive table '$tableName' are generally not used with spark.read.table(). Only 'query' option considered for direct SQL.")
    }

    // Option to read using a direct SQL query if provided in options
    options.get("query") match {
      case Some(sqlQuery) =>
        println(s"Reading Hive data using provided query: $sqlQuery")
        spark.sql(sqlQuery)
      case None =>
        println(s"Reading Hive table: $tableName")
        spark.read.table(tableName)
    }
  }
}

/**
 * Companion object for HiveReader.
 */
object HiveReader {
  // Factory methods or constants can be added here if needed.
  // For example, to construct a HiveReader or define common Hive options/properties.
}
