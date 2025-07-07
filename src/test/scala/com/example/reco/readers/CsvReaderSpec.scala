package com.example.reco.readers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CsvReaderSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("CsvReaderSpec")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "1") // Keep it small for local tests
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  "CsvReader" should "read a CSV file with header and infer schema" in {
    val csvReader = new CsvReader()
    val filePath = getClass.getResource("/test_data.csv").getPath

    val df = csvReader.read(
      path = filePath,
      options = Map("header" -> "true", "inferSchema" -> "true")
    )

    df.count() should be (3)
    df.columns should contain allOf ("id", "name", "value")

    // Check inferred schema (Spark might infer Long for id, Double for value)
    val expectedSchema = StructType(Array(
      StructField("id", IntegerType, nullable = true), // Spark might infer Integer or Long
      StructField("name", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true)
    ))

    // Comparing schema field by field as direct StructType comparison can be tricky
    df.schema.fields.length should be (expectedSchema.fields.length)
    df.schema.fields.zip(expectedSchema.fields).foreach { case (actual, expected) =>
      actual.name should be (expected.name)
      // DataType check can be more lenient if needed, e.g. Long vs Integer
      // For this test, if Spark infers Long for "id", this will fail.
      // A more robust test might check for compatibility or cast before comparison.
      // For now, let's assume Spark infers Integer for this small dataset with "inferSchema"
      // or be prepared to adjust if it infers LongType.
      if (actual.name == "id") { // Specific check for id as inferSchema can be Integer or Long
         actual.dataType should (be (IntegerType) or be (org.apache.spark.sql.types.LongType))
      } else {
         actual.dataType should be (expected.dataType)
      }
      actual.nullable should be (expected.nullable)
    }
  }

  it should "read a CSV file with a provided schema" in {
    val csvReader = new CsvReader()
    val filePath = getClass.getResource("/test_data.csv").getPath

    val providedSchema = StructType(Array(
      StructField("id_col", IntegerType, nullable = false),
      StructField("name_col", StringType, nullable = true),
      StructField("value_col", DoubleType, nullable = true)
    ))

    val df = csvReader.read(
      path = filePath,
      options = Map("header" -> "true"), // header is true, names will be from CSV
      schema = Some(providedSchema) // Schema will be applied
    )

    df.count() should be (3)
    // When schema is provided, column names from schema are used if CSV header doesn't match,
    // but if header=true and schema is provided, Spark uses header names and applies schema types.
    // Let's check if the original names are kept and types are applied.
    df.columns should contain allOf ("id", "name", "value")

    df.schema("id").dataType should be (IntegerType)
    df.schema("name").dataType should be (StringType)
    df.schema("value").dataType should be (DoubleType)

    // Collect and check data to ensure types were applied correctly (e.g., numeric parsing)
    val rows = df.collect()
    rows.length should be (3)
    rows(0).getAs[Int]("id") should be (1)
    rows(0).getAs[String]("name") should be ("ProductA")
    rows(0).getAs[Double]("value") should be (10.5 +- 0.001)
  }
}
