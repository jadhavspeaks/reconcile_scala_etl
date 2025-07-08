package com.example.reconciler

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSessionTestWrapper extends BeforeAndAfterAll { self: Suite =>

  @transient private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession.builder()
      .master("local[2]")
      .appName("ReconTest")
      .config("spark.sql.shuffle.partitions", "4") // Keep it small for tests
      .config("spark.sql.warehouse.dir", "target/spark-warehouse-test") // Avoid polluting project dir
      .enableHiveSupport() // Enable if testing Hive interactions, though may slow down basic tests
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (_spark != null) {
      _spark.stop()
      _spark = null
    }
    super.afterAll()
    // Optionally clean up warehouse dir if created:
    // import scala.reflect.io.Directory
    // import java.io.File
    // val warehouseLocation = new File("target/spark-warehouse-test")
    // val directory = new Directory(warehouseLocation)
    // if (directory.exists) {
    //   directory.deleteRecursively()
    // }
  }
}
