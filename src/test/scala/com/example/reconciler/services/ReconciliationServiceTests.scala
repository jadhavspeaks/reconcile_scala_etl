package com.example.reconciler.services

// import com.example.reconciler.SparkSessionTestWrapper
// import com.example.reconciler.config._
// import com.example.reconciler.models._
// import org.scalatest.funsuite.AnyFunSuite
// import org.scalatest.matchers.should.Matchers
// import org.apache.spark.sql.types._
// import org.apache.spark.sql.Row

// class ReconciliationServiceTests extends AnyFunSuite with Matchers with SparkSessionTestWrapper {

  // // Dummy job config for tests
  // val dummyJobConfig = ReconciliationJobConfig(
  //   jobId = "testJob",
  //   jobName = "Test Job",
  //   sourceConfig = SourceFileConfig(FileSourceConfig("dummyPath", FileFormat.CSV)), // Not used directly in these unit tests
  //   targetConfig = SourceHiveTableConfig(HiveSourceConfig("db", "table")), // Not used
  //   primaryKeyColumns = Seq("id"),
  //   columnsToCompare = Seq(ReconColumnConfig("id"))
  // )

  // test("compareRowCounts should return Success when counts match") {
  //   implicit val ss = spark
  //   val reconService = new ReconciliationService()

  //   val data = Seq(Row(1), Row(2), Row(3))
  //   val schema = StructType(List(StructField("id", IntegerType, true)))
  //   val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  //   val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

  //   val result = reconService.compareRowCounts(df1, df2, dummyJobConfig)

  //   result.status shouldBe Success
  //   result.sourceRowCount shouldBe 3
  //   result.targetRowCount shouldBe 3
  //   result.difference shouldBe 0
  //   result.summaryMessage should include("Row counts match")
  // }

  // test("compareRowCounts should return Failure when counts mismatch") {
  //   implicit val ss = spark
  //   val reconService = new ReconciliationService()

  //   val data1 = Seq(Row(1), Row(2), Row(3))
  //   val data2 = Seq(Row(1), Row(2))
  //   val schema = StructType(List(StructField("id", IntegerType, true)))
  //   val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema)
  //   val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema)

  //   val result = reconService.compareRowCounts(df1, df2, dummyJobConfig)

  //   result.status shouldBe Failure
  //   result.sourceRowCount shouldBe 3
  //   result.targetRowCount shouldBe 2
  //   result.difference shouldBe 1
  //   result.summaryMessage should include("Row count mismatch")
  // }

  // test("compareSchemas should return Success for identical schemas") {
  //   implicit val ss = spark
  //   val reconService = new ReconciliationService()

  //   val schema = StructType(List(
  //     StructField("id", IntegerType, false),
  //     StructField("name", StringType, true),
  //     StructField("value", DoubleType, true)
  //   ))
  //   val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  //   val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  //   val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

  //   result.status shouldBe Success
  //   result.summaryMessage should include("Schemas are compatible")
  //   result.fieldComparisons.foreach(_.isMatch shouldBe true)
  //   result.fieldComparisons.length shouldBe 3
  // }

  // test("compareSchemas should return Failure for type mismatch") {
  //   implicit val ss = spark
  //   val reconService = new ReconciliationService()

  //   val schema1 = StructType(List(StructField("id", IntegerType, true), StructField("value", StringType, true)))
  //   val schema2 = StructType(List(StructField("id", IntegerType, true), StructField("value", DoubleType, true)))
  //   val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
  //   val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema2)

  //   val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

  //   result.status shouldBe Failure
  //   result.summaryMessage should include("Schema mismatch")
  //   val valueComparison = result.fieldComparisons.find(_.fieldName == "value").get
  //   valueComparison.isMatch shouldBe false
  //   valueComparison.remarks should contain ("Type mismatch")
  //   valueComparison.sourceDataType shouldBe Some("string")
  //   valueComparison.targetDataType shouldBe Some("double")
  // }

  // test("compareSchemas should handle case-insensitive field names (normalized to lower case)") {
  //   implicit val ss = spark
  //   val reconService = new ReconciliationService()

  //   // Spark itself is case insensitive for field names by default unless specified otherwise
  //   // Our comparison logic converts to lower case, so this should match.
  //   val schema1 = StructType(List(StructField("ID", IntegerType, true)))
  //   val schema2 = StructType(List(StructField("id", IntegerType, true)))
  //   val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
  //   val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema2)

  //   val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

  //   result.status shouldBe Success // Because field names are lowercased for comparison
  //   result.fieldComparisons.find(_.fieldName == "id").get.isMatch shouldBe true
  // }


  // test("compareSchemas should detect field missing in target") {
  //   implicit val ss = spark
  //   val reconService = new ReconciliationService()

  //   val schema1 = StructType(List(StructField("id", IntegerType, true), StructField("extra_field", StringType, true)))
  //   val schema2 = StructType(List(StructField("id", IntegerType, true)))
  //   val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
  //   val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema2)

  //   val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

  //   result.status shouldBe Failure
  //   val extraFieldComp = result.fieldComparisons.find(_.fieldName == "extra_field").get
  //   extraFieldComp.isMatch shouldBe false
  //   extraFieldComp.remarks should contain("Field missing in Target")
  // }

  // test("compareSchemas should detect field missing in source") {
  //   implicit val ss = spark
  //   val reconService = new ReconciliationService()

  //   val schema1 = StructType(List(StructField("id", IntegerType, true)))
  //   val schema2 = StructType(List(StructField("id", IntegerType, true), StructField("extra_field", StringType, true)))
  //   val df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
  //   val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema2)

  //   val result = reconService.compareSchemas(df1, df2, dummyJobConfig)

  //   result.status shouldBe Failure
  //   val extraFieldComp = result.fieldComparisons.find(_.fieldName == "extra_field").get
  //   extraFieldComp.isMatch shouldBe false
  //   extraFieldComp.remarks should contain("Field missing in Source")
  // }
// }
