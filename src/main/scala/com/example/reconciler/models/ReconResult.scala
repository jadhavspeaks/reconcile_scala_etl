package com.example.reconciler.models

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
  // DataReconResult will be added later
  errorMessages: Seq[String] = Seq.empty
)

// Helper to convert Spark StructField to a simpler representation
object SchemaConverter {
  def structFieldToString(sf: StructField): (String, String) = (sf.name, sf.dataType.simpleString)
  def dataTypeToString(dt: DataType): String = dt.simpleString
}
