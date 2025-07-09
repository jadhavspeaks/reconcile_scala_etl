package com.example.reconciler.services

import com.example.reconciler.config.{ReconciliationJobConfig, BusinessRuleConfig} // Added BusinessRuleConfig
import com.example.reconciler.models._
import org.apache.spark.sql.{DataFrame, Row, SparkSession} // Added Row
import org.apache.spark.sql.functions._ // Wildcard import for col, lit, udf, size, explode, struct etc.
import org.apache.spark.sql.types._ // Wildcard for StructType, ArrayType etc.
import org.slf4j.LoggerFactory // Added for logging
import scala.util.{Try, Success => TrySuccess, Failure => TryFailure}
import com.example.reconciler.util.DataFrameTransformer // For column mapping


class ReconciliationService(implicit spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(getClass)

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


  /**
   * Performs record-level matching based on primary keys.
   * Identifies matched records, source-only records, and target-only records.
   */
  def performDataMatching(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    jobConfig: ReconciliationJobConfig
  ): DataMatchingResult = {
    if (jobConfig.primaryKeyColumns.isEmpty) {
      val errorMsg = "Primary key columns must be defined for data matching."
      logger.error(errorMsg)
      // Returning DFs with original schemas but empty data and Failure status
      return DataMatchingResult(Failure, sourceDf.count(), targetDf.count(), spark.emptyDataFrame, sourceDf, targetDf, 0, sourceDf.count(), targetDf.count(), errorMsg)
    }

    val pkCols = jobConfig.primaryKeyColumns
    val joinCondition = pkCols.map(pk => sourceDf(pk) === targetDf(pk)).reduce(_ && _)

    // Select and alias columns to avoid ambiguity and facilitate comparison
    // Source columns: pk_col1, pk_col2, src_data_col1, src_data_col2
    // Target columns: pk_col1, pk_col2, tgt_data_col1, tgt_data_col2
    // We need to ensure PK columns are unambiguously named for the join,
    // and other columns are distinguishable.

    // Create unique aliases for source and target data columns to avoid collision after join
    val sourceAliasedCols = sourceDf.columns.map {
      case c if pkCols.contains(c) => col(c).alias(s"$c") // Keep PKs as is for join condition
      case c => col(c).alias(s"src_$c")
    }
    val targetAliasedCols = targetDf.columns.map {
      case c if pkCols.contains(c) => col(c).alias(s"$c") // Keep PKs as is for join condition
      case c => col(c).alias(s"tgt_$c")
    }

    // It's crucial that PK column names are identical for the join condition to work as intended with just col names
    // If sourceDf("id") === targetDf("id") is used, that's fine.
    // If we were to use a sequence of strings for join, they must be exact matches.
    // Let's ensure PK columns are selected once for the joined DF, and data columns are aliased.

    val aliasedSourceDf = sourceDf.select(sourceDf.columns.map(c => col(c).as(if (pkCols.contains(c)) c else s"src_$c")): _*)
    val aliasedTargetDf = targetDf.select(targetDf.columns.map(c => col(c).as(if (pkCols.contains(c)) c else s"tgt_$c")): _*)

    logger.info(s"Performing full outer join on PKs: ${pkCols.mkString(", ")}")
    val joinedDf = aliasedSourceDf.join(aliasedTargetDf, pkCols, "full_outer")
    logger.info("Caching joined DataFrame for matching analysis.")
    joinedDf.cache() // Cache for multiple passes

    // Conditions for different sets
    val sourcePkNullCondition = pkCols.map(pk => col(s"src_$pk").isNull).reduceOption(_ && _).getOrElse(lit(false))
    // If a PK column from source is src_id, then pkCols.map(pk => col(pk).isNull) is for the target side after join
    // Correct conditions based on how join populates nulls for non-matched keys:

    // Matched: All PK columns from source are NOT NULL AND All PK columns from target are NOT NULL
    // (In a full outer join on PKs, if a PK is null on one side, it means it didn't match from that side)
    // For joinedDf, PK columns come from aliasedSourceDf. If a target row doesn't match, its tgt_ prefixed columns and its PKs (if aliased differently) would be null.
    // If PKs are named the same and used in join seq, they will be coalesced.
    // A simpler way: check if a non-PK column from source is null or not null for source-only/target-only.

    // Let's pick one non-PK column from source and one from target to check for nullity,
    // assuming PKs themselves can't be null in their original tables (usually true).
    // A robust way is to check if all PKs from one side are non-null and all from other are null.

    // To distinguish matched, source-only, target-only after a full_outer join using PKs:
    // Matched: PK_src is NOT NULL AND PK_tgt is NOT NULL (after join, PKs might be coalesced or aliased)
    // Source-Only: PK_src is NOT NULL AND PK_tgt IS NULL
    // Target-Only: PK_src IS NULL AND PK_tgt IS NOT NULL

    // To handle coalesced PK columns directly from joinedDf:
    // We need to know which side contributed the PK if the other side is null for data columns.
    // A common strategy: Add indicator columns before join or use existence of non-key columns.

    // Simpler: Use one key PK column from each side for the conditions, assuming PKs are not nullable.
    // If sourceDf's PK `pkCols.head` is not null, the row came from source.
    // If targetDf's PK `pkCols.head` (with tgt_ prefix for data columns) is not null, row came from target.

    // For this to work cleanly, we need to ensure PK columns from target are uniquely named in the join if not coalesced.
    // If `pkCols` are `Seq("id", "date")`, join is on `aliasedSourceDf.id === aliasedTargetDf.id && aliasedSourceDf.date === aliasedTargetDf.date`
    // `joinedDf` will have columns: id, date, src_colA, src_colB, tgt_colX, tgt_colY
    // - Matched: src_colA IS NOT NULL AND tgt_colX IS NOT NULL (pick any non-PK from each side)
    // - Source-only: src_colA IS NOT NULL AND tgt_colX IS NULL
    // - Target-only: src_colA IS NULL AND tgt_colX IS NOT NULL

    val firstSrcDataCol = aliasedSourceDf.columns.find(c => !pkCols.contains(c)).getOrElse(pkCols.head) // Fallback to PK if no other data cols
    val firstTgtDataCol = aliasedTargetDf.columns.find(c => !pkCols.contains(c)).getOrElse(pkCols.head)


    val matchedDf = joinedDf.filter(col(firstSrcDataCol).isNotNull && col(firstTgtDataCol).isNotNull)
    val sourceOnlyDf = joinedDf.filter(col(firstTgtDataCol).isNull).select(aliasedSourceDf.columns.map(col):_*) // Select original source columns
    val targetOnlyDf = joinedDf.filter(col(firstSrcDataCol).isNull).select(aliasedTargetDf.columns.map(col):_*) // Select original target columns

    val sourceRowCount = sourceDf.count() // Get initial counts for summary
    val targetRowCount = targetDf.count()
    val matchedKeyCount = matchedDf.count()
    val sourceOnlyKeyCount = sourceOnlyDf.count()
    val targetOnlyKeyCount = targetOnlyDf.count()

    joinedDf.unpersist()

    val summaryMsg = s"Data Matching: Matched Keys: $matchedKeyCount, Source-Only Keys: $sourceOnlyKeyCount, Target-Only Keys: $targetOnlyKeyCount."
    logger.info(summaryMsg)

    DataMatchingResult(
      status = Success, // Or Failure if counts indicate issues beyond just presence. For now, Success if runs.
      sourceRowCount = sourceRowCount,
      targetRowCount = targetRowCount,
      matchedRecordsDf = matchedDf, // This DF contains src_ and tgt_ prefixed columns for matched keys
      sourceOnlyRecordsDf = sourceOnlyDf,
      targetOnlyRecordsDf = targetOnlyDf,
      matchedKeyCount = matchedKeyCount,
      sourceOnlyKeyCount = sourceOnlyKeyCount,
      targetOnlyKeyCount = targetOnlyKeyCount,
      summaryMessage = summaryMsg
    )
  }

  /**
   * Compares column values for records that were matched on primary keys.
   *
   * @param matchedDf DataFrame containing rows with matched PKs.
   *                  Expected to have source columns aliased as 'src_colName' and target as 'tgt_colName'.
   *                  PK columns should have their original names.
   * @param jobConfig The job configuration containing columns to compare and their rules.
   * @return A ValueComparisonResult containing statistics and a DataFrame of mismatched records with details.
   */
  def compareColumnValues(
    matchedDf: DataFrame, // This DF is the output of performDataMatching's matchedRecordsDf
    jobConfig: ReconciliationJobConfig
  ): ValueComparisonResult = {
    if (jobConfig.columnsToCompare.isEmpty) {
      logger.warn("No columns configured for value comparison.")
      return ValueComparisonResult(Success, spark.emptyDataFrame, matchedDf.count(), 0, Map.empty, "No columns configured for comparison.")
    }

    val pkCols = jobConfig.primaryKeyColumns
    val columnsToCompare = jobConfig.columnsToCompare.filterNot(_.isPrimaryKey) // Don't compare PKs themselves for value mismatch here

    // Define a UDF that compares a source and target value based on column config and returns mismatch details
    // This UDF will return an array of ColumnMismatchDetail structs for a given row
    val compareRowUDF = udf((row: Row) => {
      val mismatches = scala.collection.mutable.ArrayBuffer[ColumnMismatchDetail]()
      columnsToCompare.foreach { colConf =>
        val colName = colConf.columnName
        val srcColName = s"src_$colName"
        val tgtColName = s"tgt_$colName"

        // Safely get values from the Row object
        val srcValOpt = Try(row.getAs[Any](srcColName)).toOption.flatMap(Option(_))
        val tgtValOpt = Try(row.getAs[Any](tgtColName)).toOption.flatMap(Option(_))

        var isMismatch = false
        var remark = ""

        // 1. Null handling (basic: null != value, null == null can be added based on config)
        if (srcValOpt.isEmpty && tgtValOpt.isDefined) {
          isMismatch = true
          remark = "Source is null, Target is not"
        } else if (srcValOpt.isDefined && tgtValOpt.isEmpty) {
          isMismatch = true
          remark = "Source is not null, Target is null"
        } else if (srcValOpt.isDefined && tgtValOpt.isDefined) {
          val srcVal = srcValOpt.get
          val tgtVal = tgtValOpt.get
          val tolerance = colConf.tolerance.getOrElse(0.0)

          // Type-aware comparison
          (srcVal, tgtVal) match {
            case (s: String, t: String) =>
              var sComp = if (colConf.trimWhitespace.getOrElse(true)) s.trim else s
              var tComp = if (colConf.trimWhitespace.getOrElse(true)) t.trim else t
              if (colConf.ignoreCase.getOrElse(false)) {
                sComp = sComp.toLowerCase
                tComp = tComp.toLowerCase
              }
              if (sComp != tComp) {
                isMismatch = true
                remark = "String mismatch"
              }
            case (sNum: Number, tNum: Number) => // Handles Int, Long, Double, Float, BigDecimal, BigInt
              val sDecimal = BigDecimal(sNum.toString) // Convert to BigDecimal for consistent comparison
              val tDecimal = BigDecimal(tNum.toString)
              if ((sDecimal - tDecimal).abs > BigDecimal(tolerance)) {
                isMismatch = true
                remark = s"Numeric mismatch beyond tolerance $tolerance"
              } else if (sNum.toString != tNum.toString && tolerance > 0) { // Within tolerance but different string forms
                remark = s"Numerically within tolerance $tolerance, strings differ ('${sNum.toString}' vs '${tNum.toString}')"
                // isMismatch remains false if only numeric value matters
              }
            case (sBool: Boolean, tBool: Boolean) =>
              if (sBool != tBool) {
                isMismatch = true
                remark = "Boolean mismatch"
              }
            case (sDate: java.sql.Date, tDate: java.sql.Date) =>
              if (sDate.compareTo(tDate) != 0) {
                isMismatch = true
                remark = "Date mismatch"
              }
            case (sTs: java.sql.Timestamp, tTs: java.sql.Timestamp) =>
              // Timestamps can have nanosecond precision. Default .equals() should work.
              // Consider if tolerance (e.g. to the second) is needed for timestamps. For now, exact match.
              if (!sTs.equals(tTs)) {
                isMismatch = true
                remark = "Timestamp mismatch"
              }
            // TODO: Add more specific type handling, especially for string-to-date/ts parsing using colConf.format

            // Fallback to string comparison if types are different or not explicitly handled
            // This part needs to be more intelligent based on schema or expected common type
            case _ =>
              var s1 = srcVal.toString
              var s2 = tgtVal.toString
              if (colConf.trimWhitespace.getOrElse(true)) {
                s1 = s1.trim
                s2 = s2.trim
              }
              if (colConf.ignoreCase.getOrElse(false)) {
                s1 = s1.toLowerCase
                s2 = s2.toLowerCase
              }
              if (s1 != s2) {
                isMismatch = true
                remark = "General value mismatch (possibly due to differing types or unhandled type comparison)"
              }
          }
        }
        // If isMismatch is still false after null checks, it means srcValOpt and tgtValOpt were both None, which is a match (for null_equals_null=true, this needs adjustment)

        if (isMismatch) {
          mismatches += ColumnMismatchDetail(colName, srcValOpt.map(_.toString), tgtValOpt.map(_.toString), remark)
        }
      }
      if (mismatches.isEmpty) null else mismatches.toArray // Return null if no mismatches, else array of details
    })

    // Define the schema for the UDF output (array of structs)
    val mismatchDetailSchema = ArrayType(StructType(Seq(
      StructField("columnName", StringType, false),
      StructField("sourceValue", StringType, true),
      StructField("targetValue", StringType, true),
      StructField("remark", StringType, false)
    )))

    // Apply the UDF to each row of the matchedDf
    // The UDF needs all src_ and tgt_ columns used in comparison + PKs
    // It's easier to pass the whole row to the UDF.
    val allComparedColsForUDF = pkCols ++ columnsToCompare.flatMap(c => Seq(s"src_${c.columnName}", s"tgt_${c.columnName}"))

    // Ensure all columns needed by UDF are present in struct passed to UDF
    val columnsForStruct = matchedDf.columns.filter(c => pkCols.contains(c) || c.startsWith("src_") || c.startsWith("tgt_"))


    val withMismatchDetailsDf = matchedDf.withColumn("mismatch_details_array", compareRowUDF(struct(columnsForStruct.map(col): _*)))

    val mismatchedRecordsDf = withMismatchDetailsDf
      .filter(col("mismatch_details_array").isNotNull && size(col("mismatch_details_array")) > 0)
      .select((pkCols.map(col) :+ col("mismatch_details_array").alias("mismatches")): _*) // Select PKs and mismatch details

    mismatchedRecordsDf.cache()
    val mismatchedRowCount = mismatchedRecordsDf.count()
    val totalComparedRows = matchedDf.count()

    // Calculate per-column mismatch counts (more complex, might need to explode array and group)
    // For now, a simple overall mismatch count.
    val columnMismatchCounts: Map[String, Long] = if (mismatchedRowCount > 0) {
        mismatchedRecordsDf.select(explode(col("mismatches")).alias("m_detail"))
          .groupBy("m_detail.columnName")
          .count()
          .collect()
          .map(row => row.getString(0) -> row.getLong(1))
          .toMap
      } else {
        Map.empty[String, Long]
      }

    mismatchedRecordsDf.unpersist()

    val status = if (mismatchedRowCount == 0) Success else Failure
    val summaryMsg = s"Value Comparison: $mismatchedRowCount rows out of $totalComparedRows matched rows have discrepancies. " +
      s"Column Mismatches: ${columnMismatchCounts.map{case (k,v) => s"$k($v)"}.mkString(", ")}"

    ValueComparisonResult(
      status,
      mismatchedRecordsDf, // DataFrame of mismatched rows with PKs and mismatch_details_array
      totalComparedRows,
      mismatchedRowCount,
      columnMismatchCounts,
      summaryMsg
    )
  }

  /**
   * Executes a sequence of business rule SQL queries and compares their results against expected values.
   *
   * @param rules The sequence of BusinessRuleConfig objects.
   * @param spark The implicit SparkSession.
   * @return A sequence of BusinessRuleResult objects.
   */
  def executeBusinessRules(
    rules: Seq[BusinessRuleConfig]
  )(implicit spark: SparkSession): Seq[BusinessRuleResult] = {
    // This method now only handles the "legacy" business rules that compare against a literal expectedResult.
    // The new business rule vs. target comparison is handled by `executeAndCompareBusinessRule`.
    rules.filter(rule => rule.expectedResult.isDefined || rule.joinKeysForTargetComparison.isEmpty).map { rule => // Process only if expectedResult is there OR no target join keys
      logger.info(s"Executing legacy business rule (vs literal): ${rule.ruleName} - Query: ${rule.sqlQuery}")
      Try {
        val df = spark.sql(rule.sqlQuery)
        val actualValueOpt: Option[String] = df.collect().headOption.flatMap { row =>
          if (row.length > 0) Some(String.valueOf(row.get(0))) else None
        }

        val (status, remark) = (rule.expectedResult, actualValueOpt) match {
          case (Some(expected), Some(actual)) =>
            if (expected.trim == actual.trim) (Success, Some("Result matches expected value."))
            else (Failure, Some(s"Mismatch: Expected '$expected', Actual '$actual'"))
          case (None, Some(actual)) => // Rule ran, got a result, but no expected result to compare against for this type of rule
             (Success, Some(s"Rule executed, actual result: '$actual'. No literal expected result defined for direct comparison for rule type."))
          case (Some(expected), None) =>
            (Failure, Some(s"Expected '$expected', but got no result (None)."))
          case (None, None) => // Rule ran, no result, no expected result.
            (Success, Some("Rule executed, no result returned. No literal expected result defined."))
        }
        BusinessRuleResult(rule.ruleName, status, rule.sqlQuery, rule.expectedResult, actualValueOpt, remark)
      } match {
        case TrySuccess(result) => result
        case TryFailure(ex) =>
          logger.error(s"Failed to execute legacy business rule '${rule.ruleName}': ${ex.getMessage}", ex)
          BusinessRuleResult(rule.ruleName, Failure, rule.sqlQuery, rule.expectedResult, None, Some(s"Execution error: ${ex.getMessage}"))
      }
    }
  }

  /**
   * Executes a business rule SQL, then compares its result DataFrame against a target DataFrame.
   *
   * @param rule The BusinessRuleConfig containing the SQL query and target join keys.
   * @param targetDf The target DataFrame to compare against.
   * @param jobConfig The overall job configuration, used for column mapping and comparison definitions.
   * @return An Option[ValueComparisonResult] summarizing the comparison. None if setup is invalid.
   */
  def executeAndCompareBusinessRule(
    rule: BusinessRuleConfig,
    targetDf: DataFrame,
    jobConfig: ReconciliationJobConfig
  )(implicit spark: SparkSession): Option[ValueComparisonResult] = {
    logger.info(s"Executing business rule for comparison against target: ${rule.ruleName} - Query: ${rule.sqlQuery}")

    rule.joinKeysForTargetComparison match {
      case Some(joinKeys) if joinKeys.nonEmpty =>
        Try {
          var businessRuleResultDf = spark.sql(rule.sqlQuery)
          logger.info(s"Business rule SQL query executed. Result schema for '${rule.ruleName}':")
          businessRuleResultDf.printSchema()

          // Apply column mapping if provided in jobConfig to align businessRuleResultDf with target naming conventions
          val mappedBusinessRuleResultDf = jobConfig.columnNameMapping match {
            case Some(mapping) if mapping.nonEmpty =>
              logger.info(s"Applying column mapping to business rule result for '${rule.ruleName}'")
              DataFrameTransformer.applyColumnMapping(businessRuleResultDf, mapping)
            case _ =>
              logger.info(s"No column mapping defined or mapping is empty. Using business rule result columns as is for '${rule.ruleName}'.")
              businessRuleResultDf
          }
          logger.info(s"Schema of (mapped) business rule result for '${rule.ruleName}' before aliasing for comparison:")
          mappedBusinessRuleResultDf.printSchema()

          // Prepare business rule result DF for comparison (prefixing columns with "src_")
          // PK columns for comparison should be the joinKeysForTargetComparison
          // Data columns for comparison come from jobConfig.columnsToCompare
          val aliasedBusinessRuleResultDf = mappedBusinessRuleResultDf.select(
            (joinKeys ++ jobConfig.columnsToCompare.map(_.columnName)).distinct
              .map(c => mappedBusinessRuleResultDf(c).alias(if (joinKeys.contains(c)) c else s"src_$c")): _*
          )
          // Ensure all join key columns are present in aliasedBusinessRuleResultDf
          joinKeys.foreach{ keyCol =>
            if (!aliasedBusinessRuleResultDf.columns.contains(keyCol)) {
                 throw new IllegalArgumentException(s"Join key column '$keyCol' not found in the (mapped) business rule result for rule '${rule.ruleName}'. Available columns: ${aliasedBusinessRuleResultDf.columns.mkString(", ")}")
            }
          }


          // Prepare target DF for comparison (prefixing columns with "tgt_")
          val aliasedTargetDf = targetDf.select(
            (joinKeys ++ jobConfig.columnsToCompare.map(_.columnName)).distinct
              .map(c => targetDf(c).alias(if (joinKeys.contains(c)) c else s"tgt_$c")): _*
          )
           // Ensure all join key columns are present in aliasedTargetDf
          joinKeys.foreach{ keyCol =>
            if (!aliasedTargetDf.columns.contains(keyCol)) {
                 throw new IllegalArgumentException(s"Join key column '$keyCol' not found in the target DataFrame for rule '${rule.ruleName}'. Available columns: ${aliasedTargetDf.columns.mkString(", ")}")
            }
          }


          logger.info(s"Joining business rule result with target on keys: ${joinKeys.mkString(", ")} for rule '${rule.ruleName}'")
          // Perform an inner join: we only care about records present in both for value comparison based on this rule type
          val joinedForComparisonDf = aliasedBusinessRuleResultDf.join(aliasedTargetDf, joinKeys, "inner")
          logger.info(s"Join for business rule '${rule.ruleName}' completed. Found ${joinedForComparisonDf.count()} records to compare values.")

          if (joinedForComparisonDf.isEmpty) {
             logger.warn(s"No records found after joining business rule result with target for rule '${rule.ruleName}'. Skipping value comparison.")
             // Return a successful ValueComparisonResult with 0 rows compared, 0 mismatches
             Some(ValueComparisonResult(Success, spark.emptyDataFrame, 0, 0, Map.empty, "No common records found between business rule result and target based on join keys."))
          } else {
              // Now call the existing compareColumnValues logic.
              // We need to ensure jobConfig used here has primaryKeyColumns set to `joinKeys` for this specific call.
              val tempJobConfigForComparison = jobConfig.copy(primaryKeyColumns = joinKeys)
              Some(compareColumnValues(joinedForComparisonDf, tempJobConfigForComparison))
          }

        } match {
          case TrySuccess(result) => result
          case TryFailure(ex) =>
            logger.error(s"Failed to execute and compare business rule '${rule.ruleName}' against target: ${ex.getMessage}", ex)
            Some(ValueComparisonResult(Failure, spark.emptyDataFrame, 0, 0, Map.empty, s"Error comparing rule '${rule.ruleName}': ${ex.getMessage}"))
        }
      case _ =>
        logger.warn(s"Business rule '${rule.ruleName}' is configured for target comparison but 'joinKeysForTargetComparison' are missing or empty. Skipping comparison.")
        None
    }
  }
}
