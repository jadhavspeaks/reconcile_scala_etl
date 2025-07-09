package com.example.reconciler.config

import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.ext.EnumNameSerializer // Reverted: Removed ShortClassNameTypeHints import
import scala.util.Try

class OracleConfigFetcherTests extends FunSuite with Matchers {

  implicit val formatsForTest: Formats = DefaultFormats +
    new EnumNameSerializer(FileFormat) +
    FieldSerializer[SourceFileConfig]() + // Reverted
    FieldSerializer[SourceHiveTableConfig]() // Reverted

  test("fetchConfig should parse valid JSON into ReconciliationJobConfig (simulated)") {
    val jobId = "testJob123"
    val sampleJson = OracleConfigFetcher.generateSampleJsonForJob(jobId)

    // This test focuses on parsing, not the HTTP call itself.
    // We directly parse the generated sample JSON.
    val parsedConfig = Try(parse(sampleJson).extract[ReconciliationJobConfig]).toOption

    parsedConfig.isDefined should be (true)
    parsedConfig.get.jobId should be (jobId)
    parsedConfig.get.jobName should be (s"Sample $jobId Type Reconciliation")

    // Check sourceConfig type and basic fields
    assert(parsedConfig.get.sourceConfig.isInstanceOf[SourceFileConfig])
    val sourceFileCfg = parsedConfig.get.sourceConfig.asInstanceOf[SourceFileConfig].fileConfig
    sourceFileCfg.path should be (s"hdfs:///user/data/input/$jobId.csv")
    sourceFileCfg.format should be (FileFormat.CSV)
    sourceFileCfg.customSchema.isDefined should be (true)
    sourceFileCfg.customSchema.get.length should be (4)
    sourceFileCfg.customSchema.get.head.name should be ("id")
    sourceFileCfg.customSchema.get.head.dataType should be ("IntegerType")
    sourceFileCfg.customSchema.get.head.nullable should be (false)

    // Check targetConfig type and basic fields
    assert(parsedConfig.get.targetConfig.isInstanceOf[SourceHiveTableConfig])
    val targetHiveCfg = parsedConfig.get.targetConfig.asInstanceOf[SourceHiveTableConfig].hiveConfig
    targetHiveCfg.tableName should be (s"${jobId}_target_table")

    parsedConfig.get.columnsToCompare.length should be (4)
    parsedConfig.get.columnsToCompare.head.columnName should be ("id")
    parsedConfig.get.columnsToCompare.head.isPrimaryKey should be (true)

    parsedConfig.get.emailNotifications.isDefined should be (true)
    parsedConfig.get.emailNotifications.get.recipients should contain ("test@example.com")

    parsedConfig.get.timeoutSeconds should be (Some(120))
  }

  test("JSON parsing should fail for invalid JSON structure") {
    val invalidJson = """{"jobId":"badJob", "jobName":"This is not a valid structure for our case class" }"""
    // Expect a MappingException when trying to extract to ReconciliationJobConfig
    intercept[MappingException] { // Changed from assertThrows
      parse(invalidJson).extract[ReconciliationJobConfig]
    }
  }

  test("JSON parsing should fail for unparseable JSON") {
    val unparseableJson = """{"jobId":"broken", "this is not json,,,, }"""
    intercept[org.json4s.ParserUtil.ParseException] { // Changed from assertThrows
      parse(unparseableJson).extract[ReconciliationJobConfig]
    }
  }

  // The HTTP call test is still deferred as it requires mocking or a test server.
  // test("fetchConfig with a non-existent API URL should return None") {
  //   val nonExistentApiUrl = "http://localhost:12345/api/config"
  //   val config = OracleConfigFetcher.fetchConfig("someJobId", nonExistentApiUrl)
  //   config should be (None)
  // }
}
