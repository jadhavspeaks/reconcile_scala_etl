package com.example.reconciler.util

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object DataFrameTransformer {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Renames columns in a DataFrame based on the provided mapping.
   *
   * @param df The input DataFrame.
   * @param columnNameMapping A map where keys are current column names (source)
   *                          and values are new column names (target).
   * @return A new DataFrame with specified columns renamed.
   */
  def applyColumnMapping(df: DataFrame, columnNameMapping: Map[String, String]): DataFrame = {
    if (columnNameMapping.isEmpty) {
      logger.info("Column name mapping is empty. Returning original DataFrame.")
      return df
    }

    logger.info(s"Applying column mapping: $columnNameMapping")

    // Validate that all source columns in the mapping exist in the DataFrame
    val dfColumns = df.columns.toSet
    val missingSourceColumns = columnNameMapping.keys.filterNot(dfColumns.contains)
    if (missingSourceColumns.nonEmpty) {
      logger.warn(s"The following source columns specified in the mapping do not exist in the DataFrame and will be ignored: ${missingSourceColumns.mkString(", ")}")
    }

    // Build the list of select expressions, renaming where needed
    // Iterate over original DataFrame columns to maintain order and include unmapped columns
    val selectExprs = df.columns.map { colName =>
      columnNameMapping.get(colName) match {
        case Some(targetName) =>
          if (colName != targetName) {
            logger.debug(s"Mapping column '$colName' to '$targetName'.")
            df(colName).alias(targetName)
          } else {
            df(colName) // No rename needed if source and target names are the same
          }
        case None => df(colName) // Column not in mapping, keep as is
      }
    }

    val mappedDf = df.select(selectExprs: _*)

    // Log schema after mapping for debugging
    if (logger.isDebugEnabled) {
        logger.debug("Schema after applying column mapping:")
        mappedDf.printSchema()
    }
    mappedDf
  }
}
