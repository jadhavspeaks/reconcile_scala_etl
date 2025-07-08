package com.example.reconciler.services

import com.example.reconciler.SparkSessionTestWrapper
import com.example.reconciler.config._
import com.example.reconciler.models._ // Wildcard import for ReconStatus, Success, Failure etc.
import org.scalatest.funsuite.AnyFunSuite // Corrected import
import org.scalatest.matchers.should.Matchers // Corrected import
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class ReconciliationServiceTests extends AnyFunSuite with Matchers with SparkSessionTestWrapper {

  // Dummy job config for tests
  val dummyJobConfig = ReconciliationJobConfig(
    jobId = "testJob",
    jobName = "Test Job",
    sourceConfig = SourceFileConfig(FileSourceConfig("dummyPath", FileFormat.CSV)), // Not used directly in these unit tests
    targetConfig = SourceHiveTableConfig(HiveSourceConfig("db", "table")), // Not used
    primaryKeyColumns = Seq("id"),
    columnsToCompare = Seq(ReconColumnConfig("id"))
  )

  test("compareRowCounts should return Success when counts match") {
    implicit val ss = spark
    val reconService = new ReconciliationService()

    val data = Seq(Row(1), Row(2), Row(3))
    val schema = StructType(List(StructField("id", IntegerType, true)))
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val result = reconService.compareRowCounts(df1, df2, dummyJobConfig)

    result.status shouldBe Success
    result.sourceRowCount shouldBe 3
    result.targetRowCount shouldBe 3
    result.difference shouldBe 0
    result.summaryMessage should include("Row counts match")
  }

  test("compareRowCounts should return Failure when counts mismatch") {
    implicit val ss = spark
    val reconService = new ReconciliationService()

    val data1 = Seq(Row(1), Row(2), Row(3))
    val data2 = Seq(Row(1), Row(2))
    val schema = StructType(List(StructField("id", IntegerType, true)))
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema)

    val result = reconService.compareRowCounts(df1, df2, dummyJobConfig)

    result.status shouldBe Failure
    result.sourceRowCount shouldBe 3
    result.targetRowCount shouldBe 2
    result.difference shouldBe 1
    result.summaryMessage should include("Row count mismatch")
  }

  test("compareSchemas should return Success for identical schemas") {
    implicit val ss = spark
    val reconService = new ReconciliationService()

    val schema = StructType(List(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true),
      StructField("value", DoubleType, true)
    ))
    val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

    result.status shouldBe Success
    result.summaryMessage should include("Schemas are compatible")
    result.fieldComparisons.foreach(_.isMatch shouldBe true)
    result.fieldComparisons.length shouldBe 3
  }

  test("compareSchemas should return Failure for type mismatch") {
    implicit val ss = spark
    val reconService = new ReconciliationService()

    val schema1 = StructType(List(StructField("id", IntegerType, true), StructField("value", StringType, true)))
    val schema2 = StructType(List(StructField("id", IntegerType, true), StructField("value", DoubleType, true)))
    val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema2)

    val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

    result.status shouldBe ReconStatus.Failure
    result.summaryMessage should include("Schema mismatch")
    val valueComparison = result.fieldComparisons.find(_.fieldName == "value").get
    valueComparison.isMatch shouldBe false
    valueComparison.remarks should contain ("Type mismatch")
    valueComparison.sourceDataType shouldBe Some("string")
    valueComparison.targetDataType shouldBe Some("double")
  }

  test("compareSchemas should handle case-insensitive field names (normalized to lower case)") {
    implicit val ss = spark
    val reconService = new ReconciliationService()

    // Spark itself is case insensitive for field names by default unless specified otherwise
    // Our comparison logic converts to lower case, so this should match.
    val schema1 = StructType(List(StructField("ID", IntegerType, true)))
    val schema2 = StructType(List(StructField("id", IntegerType, true)))
    val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema2)

    val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

    result.status shouldBe ReconStatus.Success // Because field names are lowercased for comparison
    result.fieldComparisons.find(_.fieldName == "id").get.isMatch shouldBe true
  }


  test("compareSchemas should detect field missing in target") {
    implicit val ss = spark
    val reconService = new ReconciliationService()

    val schema1 = StructType(List(StructField("id", IntegerType, true), StructField("extra_field", StringType, true)))
    val schema2 = StructType(List(StructField("id", IntegerType, true)))
    val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema2)

    val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

    result.status shouldBe ReconStatus.Failure
    val extraFieldComp = result.fieldComparisons.find(_.fieldName == "extra_field").get
    extraFieldComp.isMatch shouldBe false
    extraFieldComp.remarks should contain("Field missing in Target")
  }

  test("compareSchemas should detect field missing in source") {
    implicit val ss = spark
    val reconService = new ReconciliationService()

    val schema1 = StructType(List(StructField("id", IntegerType, true)))
    val schema2 = StructType(List(StructField("id", IntegerType, true), StructField("extra_field", StringType, true)))
    val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema2)

    val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

    result.status shouldBe ReconStatus.Failure
    val extraFieldComp = result.fieldComparisons.find(_.fieldName == "extra_field").get
    extraFieldComp.isMatch shouldBe false
    extraFieldComp.remarks should contain("Field missing in Source")
  }

  test("normalizeTypeName should correctly normalize common types") {
    // This is a private method, but its effect is tested via compareSchemas.
    // If we wanted to test it directly, we'd make it package-private or use reflection.
    // For now, relying on its use in compareSchemas is sufficient.
    // Example: "integer" vs "int" should match.
    implicit val ss = spark
    val reconService = new ReconciliationService()

    val schema1 = StructType(List(StructField("id", IntegerType, true))) // Spark reports as "int" often in simpleString
    val schema2 = StructType(List(StructField("id", IntegerType, true))) // if one df was from json infer, it might be "integer"
                                                                        // but StructField(...IntegerType...).dataType.simpleString is "int"
                                                                        // StructField(...DataTypes.IntegerType...).dataType.catalogString is "int"
                                                                        // Let's test the direct effect of our custom normalizer by having one type as "integer" string

    // Simulating schema representation if one was from a config using "integer" string
    val sourceFields = Map("id" -> "integer")
    val targetFields = Map("id" -> "int") // Spark's simpleString for IntegerType

    // This test is a bit artificial as it bypasses DataFrame schema introspection.
    // The true test is that compareSchemas works for DataFrames that might have these string variations if we were to construct StructTypes from strings.
    // However, Spark's StructType.simpleString is quite canonical.
    // "int" is the simpleString for IntegerType.
    // Let's assume our config might give "integer" and Spark gives "int".
    val schemaSrc = StructType(Seq(StructField("id", IntegerType))) // simpleString "int"
    val dfSrc = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaSrc)

    // To force a different string representation for the "same" type for testing normalization,
    // we'd have to mock deeper or rely on the existing tests where Spark might produce variations.
    // The current normalizeTypeName("integer") -> "int" ensures consistency if "integer" was a type string we accepted.
    // The current compareSchemas test for "identical schemas" implicitly covers this, as Spark's types are consistent.
    // If we were to allow users to type "Integer" vs "int" in config and want them to match Spark's "int", it would be relevant.
    // Our SparkSchemaConverter already normalizes "integertype" -> IntegerType etc.
    // The normalizeTypeName in ReconciliationService is more about normalizing Spark's own output if it had variations (e.g. "String" vs "StringType")
    // but simpleString is usually consistent. "string" for StringType, "int" for IntegerType.

    // Test with slightly different but compatible types (e.g. from different sources)
    // Spark itself might already make these compatible, the test is for our layer.
    val schemaRefined1 = StructType(List(StructField("id", DataTypes.createStructField("id", "integer", true))))
    val schemaRefined2 = StructType(List(StructField("id", DataTypes.createStructField("id", "int", true))))

    val df_ref1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaRefined1)
    val df_ref2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaRefined2)

    val result = reconService.compareSchemas(df_ref1, df_ref2, dummyJobConfig)
    result.status shouldBe ReconStatus.Success // because "integer" and "int" are normalized to "int"
    result.fieldComparisons.find(_.fieldName == "id").get.sourceDataType shouldBe Some("integer") // from createStructField
    result.fieldComparisons.find(_.fieldName == "id").get.targetDataType shouldBe Some("int")     // from createStructField
    result.fieldComparisons.find(_.fieldName == "id").get.isMatch shouldBe true
  }
}
