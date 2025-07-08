package com.example.reconciler.readers

import com.example.reconciler.config._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType, LongType, FloatType, ShortType, ByteType, DecimalType} // Add more as needed
import org.apache.spark.sql.functions.col

trait DataSourceReader {
  def read(config: DataSourceConfig)(implicit spark: SparkSession): DataFrame
}

object SparkSchemaConverter {
  def toSparkSchema(customSchema: Seq[SchemaColumnConfig]): StructType = {
    val fields = customSchema.map { colConfig =>
      val sparkType = colConfig.dataType.toLowerCase match {
        case "stringtype" | "string" => StringType
        case "integertype" | "integer" | "int" => IntegerType
        case "doubletype" | "double" => DoubleType
        case "booleantype" | "boolean" => BooleanType
        case "datetype" | "date" => DateType
        case "timestamptype" | "timestamp" => TimestampType
        case "longtype" | "long" => LongType
        case "floattype" | "float" => FloatType
        case "shorttype" | "short" => ShortType
        case "bytetype" | "byte" => ByteType
        case dt if dt.startsWith("decimaltype") || dt.startsWith("decimal") => // e.g., decimal(10,2)
          val parts = dt.replace("decimaltype", "").replace("decimal", "").trim.stripPrefix("(").stripSuffix(")").split(",")
          if (parts.length == 2) DecimalType(parts(0).trim.toInt, parts(1).trim.toInt)
          else DecimalType.SYSTEM_DEFAULT
        case _ => StringType // Default to StringType if unknown
      }
      StructField(colConfig.name, sparkType, colConfig.nullable)
    }
    StructType(fields)
  }
}

class FileDataSourceReader extends DataSourceReader {
  override def read(config: DataSourceConfig)(implicit spark: SparkSession): DataFrame = {
    config match {
      case SourceFileConfig(fileConfig) => loadFile(fileConfig)
      case _ => throw new IllegalArgumentException("Invalid config type for FileDataSourceReader")
    }
  }

  private def loadFile(config: FileSourceConfig)(implicit spark: SparkSession): DataFrame = {
    val reader = spark.read

    // Apply schema if provided
    config.customSchema.map(SparkSchemaConverter.toSparkSchema).foreach(reader.schema)
    config.inferSchema.foreach(reader.option("inferSchema", _))


    config.format match {
      case FileFormat.CSV =>
        reader.option("header", config.header.getOrElse(false))
        config.delimiter.foreach(reader.option("delimiter", _))
        reader.csv(config.path)
      case FileFormat.EXCEL =>
        reader.format("com.crealytics.spark.excel")
          .option("header", config.header.getOrElse(true)) // excel usually has headers
          .option("dataAddress", s"'${config.sheetName.getOrElse("Sheet1")}'!A1") // Default to Sheet1, A1 start
          .option("treatEmptyValuesAsNulls", "true")
          .option("inferSchema", config.inferSchema.getOrElse(false)) // excel infer schema can be tricky
          // If custom schema is provided for excel, it's applied by the general reader.schema() call.
          // However, spark-excel has its own schema handling, this might need refinement.
          // Forcing specific types for Excel might be better done *after* loading with a select + cast.
          .load(config.path)
      case FileFormat.TEXT | FileFormat.DAT => // Assuming DAT is a delimited text file
        // For TEXT, it reads each line as a 'value' column.
        // If DAT is like CSV, it should be handled as CSV.
        // This part might need more specific logic based on DAT file structure.
        // If it's fixed-width, that's a more complex parser.
        // Assuming simple text or delimited for now.
        if (config.format == FileFormat.DAT && config.delimiter.isDefined) {
           reader.option("header", config.header.getOrElse(false))
           config.delimiter.foreach(reader.option("delimiter", _))
           reader.csv(config.path) // Treat DAT as CSV if delimiter is present
        } else {
           reader.textFile(config.path).toDF("value")
        }
      case FileFormat.JSON =>
        reader.option("multiline", config.multiLine.getOrElse(false))
        reader.json(config.path)
      case FileFormat.PARQUET =>
        reader.parquet(config.path)
      case FileFormat.ORC =>
        reader.orc(config.path)
      case _ =>
        throw new UnsupportedOperationException(s"File format ${config.format} not supported yet.")
    }
  }
}

class HiveDataSourceReader extends DataSourceReader {
  override def read(config: DataSourceConfig)(implicit spark: SparkSession): DataFrame = {
    config match {
      case SourceHiveTableConfig(hiveConfig) => loadHiveTable(hiveConfig)
      case _ => throw new IllegalArgumentException("Invalid config type for HiveDataSourceReader")
    }
  }

  private def loadHiveTable(config: HiveSourceConfig)(implicit spark: SparkSession): DataFrame = {
    spark.table(s"${config.databaseName}.${config.tableName}")
  }
}

object DataSourceReaderFactory {
  def getReader(config: DataSourceConfig): DataSourceReader = {
    config match {
      case _: SourceFileConfig => new FileDataSourceReader()
      case _: SourceHiveTableConfig => new HiveDataSourceReader()
      // Potentially other types like JDBCSourceConfig in the future
      // case _: JDBCSourceConfig => new JDBCDataSourceReader()
      case _ => throw new IllegalArgumentException(s"No reader available for config type: ${config.getClass.getSimpleName}")
    }
  }
}
