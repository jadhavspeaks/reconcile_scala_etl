package com.example.reco

import org.apache.spark.sql.{DataFrame, SparkSession}

case class RowCountSummary(
  sourceAName: String,
  sourceBName: String,
  sourceARowCount: Long,
  sourceBRowCount: Long,
  difference: Long,
  isMatch: Boolean
) {
  override def toString: String = {
    s"""
      |Row Count Reconciliation Summary:
      |---------------------------------
      |Source A ($sourceAName): $sourceARowCount rows
      |Source B ($sourceBName): $sourceBRowCount rows
      |Difference: $difference rows
      |Match: ${if (isMatch) "Yes" else "No"}
      |---------------------------------
    """.stripMargin
  }
}

class ReconciliationService(spark: SparkSession) {

  /**
   * Compares the row counts of two DataFrames.
   *
   * @param dfA DataFrame from source A.
   * @param dfB DataFrame from source B.
   * @param sourceAName Optional name for source A (for reporting).
   * @param sourceBName Optional name for source B (for reporting).
   * @return RowCountSummary containing the counts and difference.
   */
  def reconcileRowCount(
    dfA: DataFrame,
    dfB: DataFrame,
    sourceAName: String = "SourceA",
    sourceBName: String = "SourceB"
  ): RowCountSummary = {
    val countA = dfA.count()
    val countB = dfB.count()
    val difference = Math.abs(countA - countB)
    val isMatch = difference == 0

    RowCountSummary(
      sourceAName = sourceAName,
      sourceBName = sourceBName,
      sourceARowCount = countA,
      sourceBRowCount = countB,
      difference = difference,
      isMatch = isMatch
    )
  }

  // Other reconciliation methods (schema, data, etc.) will be added here later.
}

object ReconciliationService {
  def apply(spark: SparkSession): ReconciliationService = new ReconciliationService(spark)
}
