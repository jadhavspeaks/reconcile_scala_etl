package com.example.reconciler.models

import org.apache.spark.sql.DataFrame // Added import
import org.apache.spark.sql.types.{DataType, StructField}

// General Status
sealed trait ReconStatus
case object Success extends ReconStatus
case object Failure extends ReconStatus
case object PartialSuccess extends ReconStatus // If some checks pass and some fail

// Schema Reconciliation Results
case class SchemaFieldComparison(
  fieldName: String,
  sourceDataType: Option[String], // Using String representation for simplicity in reporting
  targetDataType: Option[String],
  isMatch: Boolean,
  remarks: Option[String] = None // e.g., "Type mismatch", "Field missing in source/target"
)

case class SchemaReconResult(
  status: ReconStatus,
  sourceSchema: Option[Seq[(String, String)]], // List of (name, type)
  targetSchema: Option[Seq[(String, String)]], // List of (name, type)
  fieldComparisons: Seq[SchemaFieldComparison],
  summaryMessage: String
)

// Row Count Reconciliation Results
case class RowCountReconResult(
  status: ReconStatus,
  sourceRowCount: Long,
  targetRowCount: Long,
  difference: Long,
  summaryMessage: String
)

// Overall Job Summary (to be expanded)
case class ReconciliationJobSummary(
  jobId: String,
  jobName: String,
  overallStatus: ReconStatus,
  startTime: Long,
  endTime: Option[Long] = None,
  rowCountResult: Option[RowCountReconResult] = None,
  schemaReconResult: Option[SchemaReconResult] = None,
  dataMatchingResult: Option[DataMatchingResult] = None,
  valueComparisonResult: Option[ValueComparisonResult] = None, // For column value comparison results
  businessRuleResults: Option[Seq[BusinessRuleResult]] = None, // For business rule results
  errorMessages: Seq[String] = Seq.empty
)

// Results from record-level matching based on keys
case class DataMatchingResult(
  status: ReconStatus,
  sourceRowCount: Long,
  targetRowCount: Long,
  matchedRecordsDf: DataFrame, // Records matched on key, from source perspective (or coalesced)
  sourceOnlyRecordsDf: DataFrame, // Records present only in source
  targetOnlyRecordsDf: DataFrame, // Records present only in target
  matchedKeyCount: Long,
  sourceOnlyKeyCount: Long,
  targetOnlyKeyCount: Long,
  summaryMessage: String
)

// Details for a single column mismatch
case class ColumnMismatchDetail(
  columnName: String,
  sourceValue: Option[String], // Store as string for reporting consistency
  targetValue: Option[String],
  remark: String // e.g., "Value mismatch", "Tolerance mismatch"
)

// Results from comparing column values for matched records
case class ValueComparisonResult(
  status: ReconStatus,
  mismatchedRecordsDf: DataFrame, // DataFrame containing only rows with mismatches, plus details
  totalComparedRows: Long,
  mismatchedRowCount: Long,
  columnMismatchCounts: Map[String, Long], // columnName -> count of mismatches for that column
  summaryMessage: String
)

// Result of a single business rule execution
case class BusinessRuleResult(
  ruleName: String,
  status: ReconStatus, // Pass or Fail
  query: String,
  expectedResult: Option[String],
  actualResult: Option[String], // Store actual result as string for reporting
  remarks: Option[String] = None
)


// Helper to convert Spark StructField to a simpler representation
object SchemaConverter {
  def structFieldToString(sf: StructField): (String, String) = (sf.name, sf.dataType.simpleString)
  def dataTypeToString(dt: DataType): String = dt.simpleString
}
