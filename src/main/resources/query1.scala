import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer;
import scala.collection.JavaConverters._

// BEGIN Partial Codegen Scala

import org.apache.spark.sql.{Dataset, Column, Row,  SparkSession};
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._;
import com.oracle.dicom.model.configuration.ConfigProvider
import com.oracle.dicom.model.configuration.ParameterValue
import com.oracle.dicom.json.CustomObjectMapper
import com.oracle.dis.codegen.PipelineConfigProvider
import java.io.IOException

import com.oracle.dis.codegen.Pipeline
import com.oracle.dis.codegen.PipelineSparkFunc

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.LinkedHashMap;

object LOG {
  val LOGGER = LoggerFactory.getLogger("DIS_Spark_interactive_execution_log")
}



class INTERACTIVE_TEST_FLOW extends Pipeline {

  val LOGGER = LOG.LOGGER

  var spark: SparkSession = null
  var df: Dataset[Row] = null
  val sparkFunc = new PipelineSparkFunc();
    override def runPipelineSteps(session: SparkSession) : Integer = {
      this.spark = session;
      sparkFunc.spark = session;

      var startTimeMillis = System.currentTimeMillis()
      var elapsedTime : Long = 0
      Pipeline.LOGGER.info("Executing statement:")
      Pipeline.LOGGER.info("""val jdbc6626: Dataset[Row] = super.read(session, "ORACLE_SOURCE", "SOURCE_CONNECTION", "", "","SELECT * FROM \"ALPHA\".\"BANK_ADDRESS\" OFFSET 0 ROWS FETCH NEXT 2000 ROWS ONLY");""")
      val jdbc6626: Dataset[Row] = super.read(session, "ORACLE_SOURCE", "SOURCE_CONNECTION", "", "","SELECT * FROM \"ALPHA\".\"BANK_ADDRESS\" OFFSET 0 ROWS FETCH NEXT 2000 ROWS ONLY");
      elapsedTime = System.currentTimeMillis() - startTimeMillis
      Pipeline.LOGGER.info("statement elapsed time = " + elapsedTime + " ms")
      startTimeMillis = System.currentTimeMillis()
      0
      jdbc6626.createOrReplaceTempView("sampleFilterView")
      Pipeline.LOGGER.info("Executing final sample filter statements.")
      val q = "SELECT * FROM sampleFilterView TABLESAMPLE(100 ROWS)"
      println("apply sample filter : " + q)
      df = spark.sql(q)
      elapsedTime = System.currentTimeMillis() - startTimeMillis
      Pipeline.LOGGER.info("statement elapsed time = " + elapsedTime + " ms")

      0
  }
}

object INTERACTIVE_TEST_FLOW {
  val pipeline : INTERACTIVE_TEST_FLOW = new INTERACTIVE_TEST_FLOW
  def main(args: Array[String]) = {
    val LOG = pipeline.LOGGER

    // Initialize the execution parameter values
    initParameters(pipeline)
    LOG.info("Data flow execution properties: ")
    pipeline.getParamHandler.getConfigProvider.getParameterMap.entrySet.asScala.foreach {
      entry: java.util.Map.Entry[String,ParameterValue] => LOG.info (entry.getKey() + "-->" + entry.getValue().toString())
    }

    LOG.info("START: pipeline [ INTERACTIVE_TEST_FLOW ] code execution ")

    pipeline.startInteractive(spark);  // For interactive case, session already exists in global session variable "spark".
 
    LOG.info("END: pipeline [ INTERACTIVE_TEST_FLOW ] code execution ")
  }

  def initParameters(pipeline : INTERACTIVE_TEST_FLOW) = {
    val serializedConfigProvider =
"""{
  "bindings" : {
    "dataAsset.ORACLE_SOURCE" : {
      "rootObjValue" : {
        "modelType" : "Oracle Data Asset",
        "key" : "b0954607-3b99-4db0-bf55-b5bf5737c75c",
        "modelVersion" : "20190228",
        "parentRef" : { },
        "name" : "Oracle Source",
        "objectVersion" : 1,
        "externalId" : "jdbc:oracle:thin:@//den03cxv.us.oracle.com:1521/orclpdb.us.oracle.com",
        "defaultConnection" : {
          "modelType" : "OracleDb Connection",
          "key" : "1ef8a1aa-5c5d-44bc-9f36-5b87e78ee46b",
          "modelVersion" : "20190228",
          "parentRef" : { },
          "name" : "source Connection",
          "objectVersion" : 1,
          "isDefault" : false,
          "username" : "alpha",
          "password" : "a1phaOffice1_",
          "objectStatus" : 1,
          "identifier" : "SOURCE_CONNECTION"
        },
        "host" : "den03cxv.us.oracle.com",
        "port" : "1521",
        "serviceName" : "orclpdb.us.oracle.com",
        "objectSta""" +
"""tus" : 8,
        "identifier" : "ORACLE_SOURCE"
      }
    },
    "connection.ORACLE_SOURCE.SOURCE_CONNECTION" : {
      "rootObjValue" : {
        "modelType" : "OracleDb Connection",
        "key" : "1ef8a1aa-5c5d-44bc-9f36-5b87e78ee46b",
        "modelVersion" : "20190228",
        "parentRef" : { },
        "name" : "source Connection",
        "objectVersion" : 1,
        "isDefault" : false,
        "username" : "alpha",
        "password" : "a1phaOffice1_",
        "objectStatus" : 1,
        "identifier" : "SOURCE_CONNECTION"
      }
    },
    "schema.ORACLE_SOURCE.ALPHA" : {
      "rootObjValue" : {
        "modelType" : "Schema",
        "key" : "da93c323-383a-42e3-83bc-a6a52407593f",
        "modelVersion" : "20190228",
        "parentRef" : { },
        "name" : "ALPHA",
        "objectVersion" : 0,
        "externalId" : "jdbc:oracle:thin:@//den03cxv.us.oracle.com:1521/orclpdb.us.oracle.com/ALPHA",
        "binding" : {
          "resourceName" : "ALPHA"
        },
    """ +
"""    "hasContainers" : false,
        "objectStatus" : 8
      }
    },
    "dataEntity.ORACLE_SOURCE.ALPHA.BANK_ADDRESS" : {
      "rootObjValue" : {
        "modelType" : "Table Entity",
        "key" : "bfc58d08-b731-4b80-9f42-7f012414c1f7",
        "modelVersion" : "20190228",
        "parentRef" : { },
        "name" : "BANK_ADDRESS",
        "objectVersion" : 0,
        "externalId" : "jdbc:oracle:thin:@//den03cxv.us.oracle.com:1521/orclpdb.us.oracle.com/ALPHA/BANK_ADDRESS",
        "shapeId" : "b12a921ff2c93a1f86ff9138995c1f9afbea47b1",
        "entityType" : "TABLE",
        "otherTypeLabel" : "TABLE",
        "uniqueKeys" : [ {
          "modelType" : "Unique Key",
          "key" : "fc063267-f607-4027-a89e-23677f88f7a4",
          "modelVersion" : "20190228",
          "parentRef" : { },
          "attributeRefs" : [ ],
          "objectStatus" : 0
        } ],
        "foreignKeys" : [ ],
        "resourceName" : "BANK_ADDRESS",
        "objectStatus" : 8,
        "identifier""" +
"""" : "BANK_ADDRESS"
      }
    },
    "writeOperationConfig.SOURCE_1" : {
      "rootObjValue" : {
        "modelType" : "Read Operation Config",
        "key" : "7db4bbd3-04bd-47cd-a989-bff1e0afe213",
        "modelVersion" : "20190228",
        "parentRef" : { },
        "objectStatus" : 0
      }
    }
  },
  "childProviders" : { }
}"""
    try {
      val p: ConfigProvider = CustomObjectMapper.getStringAsObject(serializedConfigProvider, classOf[ConfigProvider]);
      pipeline.initParameterHandler(p);
    } catch {
      case e: IOException => {
        println("Exception when deserializing the config provider: ")
        e.printStackTrace()
        throw new RuntimeException(e)
      }
    }
  }
}



// END Partial Codegen Scala

class ColumnMetadata(columnName:String, columnType:String, position:Int) {
    override def toString() = "{\"name\":\"" + columnName + "\",\"type\":\"" + columnType + "\",\"columnID\":\"" + position + "\"}"
}

class Metadata() {
  def generateColumnMedataList(df:org.apache.spark.sql.DataFrame,columnName:String,count:Int,columnList:ListBuffer[ColumnMetadata]) = {
      val columnType = df.schema(columnName).dataType.simpleString;
      val metadataObj = new ColumnMetadata(columnName, columnType,count);
      columnList += metadataObj;
  }

  def generateMedataList(df:org.apache.spark.sql.DataFrame,columnName:String,count:Int,columnList:ListBuffer[ColumnMetadata]) = {
      val columnType = df.schema(columnName).dataType.simpleString;
      val metadataObj = new ColumnMetadata(columnName, columnType,count);
      columnList += metadataObj;
  }
}

class ColumnStatistic(columnName:String, columnType:String, minValue:Any, maxValue:Any, avgLength:Any, nulls:Long, total:Long) {
    override def toString() =
        "{\"name\":\"" + columnName + "\",\"type\":\"" + columnType +  "\",\"minValue\":\"" + minValue + "\",\"maxValue\":\"" + maxValue +  "\",\"avgLength\":" +
        avgLength  + ",\"total\":\"" + total + "\",\"nulls\":\"" + nulls + "\""
};

class Profile {
    def generateColumnProfile(df:org.apache.spark.sql.DataFrame, columnName:String, distDataSize:Int) {
        val startTime = System.currentTimeMillis();
        val escape = "`"
        // For Spark version < 2.4.0, schema fails for escaped column names with period however, select fails without escaping.
        val (escapedColName, unEscapedColName) = if(!columnName.contains('.'))
            (columnName, columnName)
        else if (columnName.startsWith(escape) && columnName.endsWith(escape) )
            (columnName, columnName.tail.dropRight(1))
        else ("%s%s%s".format(escape, columnName, escape) , columnName)

        val projectedDf = df.select(escapedColName).cache
        val total = projectedDf.count

        val columnType = projectedDf.schema(unEscapedColName).dataType.simpleString
        val statSummary = projectedDf.select(min(escapedColName), max(escapedColName), mean(length(projectedDf.col(escapedColName)))).head
        val minValue = statSummary.get(0)
        val maxValue = statSummary.get(1)
        val avgLength = statSummary.get(2)
        val nulls = projectedDf.filter(projectedDf.col(escapedColName).isNull).count
        val profileResult = new ColumnStatistic(columnName, columnType, minValue, maxValue, avgLength, nulls, total);

        println("colName:" + columnName + " SB0d28W2rlX5FNz8PgdG-PROFILE-BEGIN")

        print("(" + profileResult + ",\"distinctSampleData\":[")
        df.groupBy(escapedColName)
          .count()
          .orderBy(desc("count"))
          .toJSON.take(distDataSize).foreach(x => print(x + ","))
        println("]})")

        println("SB0d28W2rlX5FNz8PgdG-PROFILE-END")
        println("PROFILE TS: " + (System.currentTimeMillis() - startTime) + "ms")

        projectedDf.unpersist
    };
}

object Profile {
    val profile : Profile = new Profile // Create singleton instance
    def generateColumnProfile(df:org.apache.spark.sql.DataFrame, columnName:String, distDataSize:Int) {
        profile.generateColumnProfile(df, columnName, distDataSize)
    }
}

class AppMain {
    var df : Dataset[Row] = null // Will be initialized inside main method
}

object AppMain {
    val appMain : AppMain = new AppMain  // Create singleton instance which contains df variable
    def main(args:Array[String]) = {
        var startTime = System.currentTimeMillis();

        var count = 0;
        var columnList = new ListBuffer[ColumnMetadata]();
        var columnStatList = new ListBuffer[ColumnStatistic]();
        val metadata = new Metadata
        val sample = INTERACTIVE_TEST_FLOW  // pipeline instance

        var doesGetTableMetadata = false;
        var sampleDataSize = 0;

        doesGetTableMetadata = true;
        

        // Run the sample selection and create a DataFrame
        sample.main(Array())
        appMain.df = sample.pipeline.df

        println("JOB EXEC TS: " + (System.currentTimeMillis() - startTime) + "ms")
        startTime = System.currentTimeMillis();

        if (doesGetTableMetadata) {
            appMain.df.columns.foreach(columnName => {
                metadata.generateMedataList(appMain.df, columnName, count, columnList);
                count += 1;
            });

            println("KGhvc3RQb3JQs3nrW4Ph0f-METADATA-BEGIN")
            println(columnList)
            println("KGhvc3RQb3JQs3nrW4Ph0f-METADATA-END")
        } else {
            println("Skip to generate Table Metadata.")
        }

        if (sampleDataSize > 0) {
            print("ch7lPgd4Jz2MfcgxwDq-SAMPLE-BEGIN (")
            appMain.df.toJSON.take(sampleDataSize).foreach(x => print(x + ","))
            println(") ch7lPgd4Jz2MfcgxwDq-SAMPLE-END")
        } else {
            println("Skip to generate Sample Data.")
        }

        

        println("PROFILING TS: " + (System.currentTimeMillis() - startTime) + "ms")
    }
}

AppMain.main(Array())