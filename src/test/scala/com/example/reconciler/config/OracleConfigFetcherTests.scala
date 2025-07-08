package com.example.reconciler.config

// import com.example.reconciler.SparkSessionTestWrapper
// import org.scalatest.funsuite.AnyFunSuite
// import org.scalatest.matchers.should.Matchers
// import org.json4s._
// import org.json4s.native.JsonMethods._
// import org.json4s.ext.EnumNameSerializer


// class OracleConfigFetcherTests extends AnyFunSuite with Matchers with SparkSessionTestWrapper {

//   // Re-define formats here or make it accessible from OracleConfigFetcher if needed for tests
//   // For now, copy to ensure test is self-contained in checking parsing ability.
//   implicit val formatsForTest: Formats = DefaultFormats +
//     new EnumNameSerializer(FileFormat) +
//     FieldSerializer[SourceFileConfig]() +
//     FieldSerializer[SourceHiveTableConfig]()

//   test("fetchConfig should parse valid JSON into ReconciliationJobConfig") {
//     val jobId = "testJob123"
//     // Use the generateSampleJsonForJob from the object itself for consistency
//     val sampleJson = OracleConfigFetcher.generateSampleJsonForJob(jobId)

//     println(s"Sample JSON for testing parse: $sampleJson") // To see what's being parsed

//     val parsedConfig = Try(parse(sampleJson).extract[ReconciliationJobConfig]).toOption

//     parsedConfig shouldBe defined
//     parsedConfig.get.jobId shouldBe jobId
//     parsedConfig.get.jobName shouldBe s"Sample $jobId Type Reconciliation"
//     parsedConfig.get.sourceConfig shouldBe a[SourceFileConfig]

//     val sourceFileCfg = parsedConfig.get.sourceConfig.asInstanceOf[SourceFileConfig].fileConfig
//     sourceFileCfg.path shouldBe s"hdfs:///user/data/input/$jobId.csv"
//     sourceFileCfg.format shouldBe FileFormat.CSV
//     sourceFileCfg.customSchema.isDefined shouldBe true
//     sourceFileCfg.customSchema.get.length shouldBe 4
//     sourceFileCfg.customSchema.get.head.name shouldBe "id"
//     sourceFileCfg.customSchema.get.head.dataType shouldBe "IntegerType"
//     sourceFileCfg.customSchema.get.head.nullable shouldBe false

//     parsedConfig.get.targetConfig shouldBe a[SourceHiveTableConfig]
//     val targetHiveCfg = parsedConfig.get.targetConfig.asInstanceOf[SourceHiveTableConfig].hiveConfig
//     targetHiveCfg.tableName shouldBe s"${jobId}_target_table"

//     parsedConfig.get.columnsToCompare.length shouldBe 4
//     parsedConfig.get.columnsToCompare.head.columnName shouldBe "id"
//     parsedConfig.get.columnsToCompare.head.isPrimaryKey shouldBe true

//     parsedConfig.get.emailNotifications.isDefined shouldBe true
//     parsedConfig.get.emailNotifications.get.recipients should contain("test@example.com")

//     parsedConfig.get.timeoutSeconds shouldBe Some(120)
//   }

//   test("fetchConfig should return None for invalid JSON structure") {
//     val invalidJson = """{"jobId":"badJob", "jobName":"This is not a valid structure" }"""
//     val parsedConfig = Try(parse(invalidJson).extract[ReconciliationJobConfig]).toOption
//     // Depending on how strict json4s is, this might throw an exception caught by Try
//     // or result in a partially filled object if default values are used,
//     // or None if extraction fails cleanly.
//     // A proper test would check for MappingException.
//     // For now, let's assume it results in None due to Try wrapping.
//      Try(parse(invalidJson).extract[ReconciliationJobConfig]) match {
//       case scala.util.Success(_) => fail("Parsing invalid JSON should have failed")
//       case scala.util.Failure(e) => e shouldBe a[MappingException]
//     }
//   }

//   test("fetchConfig should return None for unparseable JSON") {
//     val unparseableJson = """{"jobId":"broken", "this is not json,,,, }"""
//      Try(parse(unparseableJson).extract[ReconciliationJobConfig]) match {
//       case scala.util.Success(_) => fail("Parsing unparseable JSON should have failed")
//       case scala.util.Failure(e) => e shouldBe a[org.json4s.ParserUtil.ParseException] // or a more general JsonParser.ParseException
//     }
//   }

//   // Testing the actual HTTP call part is harder without mocks or a test server.
//   // This test verifies that a non-existent URL (simulating network error) results in None.
//   test("fetchConfig with a non-existent API URL should return None") {
//     // Using a URL that is highly unlikely to exist or respond quickly
//     val nonExistentApiUrl = "http://localhost:12345/api/config"
//     val config = OracleConfigFetcher.fetchConfig("someJobId", nonExistentApiUrl)
//     config shouldBe None
//   }

//   // Placeholder for testing with a mock HTTP client if one were introduced
//   // test("fetchConfig should make correct HTTP call and parse response (with mock)") { ... }
// }
