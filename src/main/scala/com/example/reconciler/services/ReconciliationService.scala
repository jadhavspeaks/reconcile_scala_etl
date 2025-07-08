package com.example.reconciler.services

import com.example.reconciler.config.ReconciliationJobConfig
import com.example.reconciler.models._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import scala.util.{Try, Success => TrySuccess, Failure => TryFailure}

class ReconciliationService(implicit spark: SparkSession) {

  /**
   * Performs row count comparison between source and target DataFrames.
   */
  def compareRowCounts(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    jobConfig: ReconciliationJobConfig // For context, if needed later
  ): RowCountReconResult = {
    Try {
      val sourceCount = sourceDf.count()
      val targetCount = targetDf.count()
      val diff = sourceCount - targetCount

      val status = if (diff == 0) Success else Failure
      val message = if (diff == 0) {
        s"Row counts match: $sourceCount rows."
      } else {
        s"Row count mismatch: Source has $sourceCount rows, Target has $targetCount rows. Difference: $diff."
      }
      RowCountReconResult(status, sourceCount, targetCount, diff, message)
    } match {
      case TrySuccess(result) => result
      case TryFailure(ex) =>
        RowCountReconResult(Failure, -1, -1, -1, s"Error during row count: ${ex.getMessage}")
    }
  }

  /**
   * Performs schema reconciliation between source and target DataFrames.
   * Compares column names and data types.
   */
  def compareSchemas(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    jobConfig: ReconciliationJobConfig // For context, e.g., case sensitivity config
  ): SchemaReconResult = {
    Try {
      val sourceSchema = sourceDf.schema
      val targetSchema = targetDf.schema

      val sourceFields = sourceSchema.fields.map(f => f.name.toLowerCase -> f.dataType.simpleString).toMap
      val targetFields = targetSchema.fields.map(f => f.name.toLowerCase -> f.dataType.simpleString).toMap

      val allFieldNames = (sourceFields.keys ++ targetFields.keys).toSeq.distinct.sorted

      val comparisons = allFieldNames.map { fieldName =>
        val srcTypeOpt = sourceFields.get(fieldName)
        val tgtTypeOpt = targetFields.get(fieldName)

        (srcTypeOpt, tgtTypeOpt) match {
          case (Some(srcType), Some(tgtType)) =>
            if (normalizeTypeName(srcType) == normalizeTypeName(tgtType)) {
              SchemaFieldComparison(fieldName, Some(srcType), Some(tgtType), isMatch = true, remarks = Some("Match"))
            } else {
              SchemaFieldComparison(fieldName, Some(srcType), Some(tgtType), isMatch = false, remarks = Some(s"Type mismatch: Source is $srcType, Target is $tgtType"))
            }
          case (Some(srcType), None) =>
            SchemaFieldComparison(fieldName, Some(srcType), None, isMatch = false, remarks = Some("Field missing in Target"))
          case (None, Some(tgtType)) =>
            SchemaFieldComparison(fieldName, None, Some(tgtType), isMatch = false, remarks = Some("Field missing in Source"))
          case (None, None) =>
            // Should not happen if allFieldNames is derived correctly
            SchemaFieldComparison(fieldName, None, None, isMatch = false, remarks = Some("Field missing in both? (Error)"))
        }
      }

      val mismatches = comparisons.count(!_.isMatch)
      val status = if (mismatches == 0) Success else Failure
      val message = if (status == Success) {
        s"Schemas are compatible. All ${allFieldNames.size} fields considered match (by name and type)."
      } else {
        s"Schema mismatch: $mismatches differences found out of ${allFieldNames.size} fields considered."
      }

      SchemaReconResult(
        status,
        Some(sourceSchema.fields.map(f => (f.name, f.dataType.simpleString))),
        Some(targetSchema.fields.map(f => (f.name, f.dataType.simpleString))),
        comparisons,
        message
      )
    } match {
      case TrySuccess(result) => result
      case TryFailure(ex) =>
        SchemaReconResult(Failure, None, None, Seq.empty, s"Error during schema comparison: ${ex.getMessage}")
    }
  }

  /**
   * Normalizes Spark data type names for comparison.
   * e.g., "integer" and "int" should be treated the same.
   * Spark's simpleString is usually quite canonical, but this provides a hook if needed.
   * For example, decimal(10,0) vs decimal(10,2) are different and should be caught.
   */
  private def normalizeTypeName(typeName: String): String = {
    typeName.toLowerCase match {
      case "integer" => "int"
      // Add other normalizations if necessary, e.g., for variations of decimal, char, varchar
      case t => t
    }
  }

  // Placeholder for full data reconciliation
  // def performFullDataReconciliation(...)
}
