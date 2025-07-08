package com.example.reconciler.config

import com.example.reconciler.SparkSessionTestWrapper // Required for implicit SparkSession for fetchConfig
import org.scalatest.funsuite.AnyFunSuite // Corrected import
import org.scalatest.matchers.should.Matchers // Corrected import
import org.apache.spark.sql.types._

class SparkSchemaConverterSuite extends AnyFunSuite with Matchers {

  test("toSparkSchema should convert SchemaColumnConfig to StructType correctly") {
    val customSchema = Seq(
      SchemaColumnConfig("id", "IntegerType", nullable = false),
      SchemaColumnConfig("name", "StringType"),
      SchemaColumnConfig("value", "DoubleType"),
      SchemaColumnConfig("event_date", "DateType", format = Some("yyyy-MM-dd")),
      SchemaColumnConfig("created_at", "TimestampType"),
      SchemaColumnConfig("is_active", "BooleanType"),
      SchemaColumnConfig("amount", "DecimalType(10,2)"),
      SchemaColumnConfig("short_code", "ShortType"),
      SchemaColumnConfig("byte_val", "ByteType"),
      SchemaColumnConfig("long_id", "LongType"),
      SchemaColumnConfig("float_val", "FloatType"),
      SchemaColumnConfig("unknown_type", "MyCustomType") // Should default to StringType
    )

    val expectedSparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true),
      StructField("event_date", DateType, nullable = true),
      StructField("created_at", TimestampType, nullable = true),
      StructField("is_active", BooleanType, nullable = true),
      StructField("amount", DecimalType(10,2), nullable = true),
      StructField("short_code", ShortType, nullable = true),
      StructField("byte_val", ByteType, nullable = true),
      StructField("long_id", LongType, nullable = true),
      StructField("float_val", FloatType, nullable = true),
      StructField("unknown_type", StringType, nullable = true)
    ))

    val actualSparkSchema = SparkSchemaConverter.toSparkSchema(customSchema)
    actualSparkSchema shouldEqual expectedSparkSchema
  }

  test("toSparkSchema should handle empty input") {
    val customSchema = Seq.empty[SchemaColumnConfig]
    val expectedSparkSchema = StructType(Seq.empty[StructField])
    val actualSparkSchema = SparkSchemaConverter.toSparkSchema(customSchema)
    actualSparkSchema shouldEqual expectedSparkSchema
  }

  test("toSparkSchema should handle various casings for type names") {
     val customSchema = Seq(
      SchemaColumnConfig("col1", "integertype"),
      SchemaColumnConfig("col2", "STRING"),
      SchemaColumnConfig("col3", "boolean"),
      SchemaColumnConfig("col4", "Decimal(5,0)")
    )
    val expectedSparkSchema = StructType(Seq(
      StructField("col1", IntegerType, true),
      StructField("col2", StringType, true),
      StructField("col3", BooleanType, true),
      StructField("col4", DecimalType(5,0), true)
    ))
    val actualSparkSchema = SparkSchemaConverter.toSparkSchema(customSchema)
    actualSparkSchema shouldEqual expectedSparkSchema
  }
}

class OracleConfigFetcherSuite extends AnyFunSuite with Matchers with SparkSessionTestWrapper {
  // OracleConfigFetcher.fetchConfig requires an implicit SparkSession
  // It doesn't use it in the placeholder, but the signature requires it.

  test("fetchConfig should return sample configuration for 'sampleReconJob1'") {
    implicit val ss = spark // from SparkSessionTestWrapper
    val config = OracleConfigFetcher.fetchConfig("sampleReconJob1")
    config shouldBe defined
    config.get.jobId shouldBe "sampleReconJob1"
    config.get.jobName shouldBe "Sample CSV to Hive Reconciliation"
    config.get.sourceConfig shouldBe a[SourceFileConfig]
    config.get.targetConfig shouldBe a[SourceHiveTableConfig]
    config.get.primaryKeyColumns should contain("id")
  }

  test("fetchConfig should return None for an unknown job ID") {
    implicit val ss = spark
    val config = OracleConfigFetcher.fetchConfig("unknownJobId123")
    config shouldBe None
  }
}
