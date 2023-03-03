package org.example

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.{CharType, DataType, IntegerType, StringType, StructField, StructType}




object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    val sparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample-test")
      .getOrCreate()

    val someData = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )
    val someSchema = StructType(List(
      StructField("number", IntegerType , true),
      StructField("word", StringType, true)
    ))

    //print(DataSource.lookupDataSourceV2("csv", sparkSession2.sessionState.conf).isEmpty)
    //print(sparkSession2.sessionState.conf.legacyPathOptionBehavior)
    //val paths =
    //val df = sparkSession.read.csv(paths)
    //val legacyPathOptionBehavior = sparkSession.sessionState.conf.legacyPathOptionBehavior

    var userSpecifiedSchema: Option[StructType] = None
    var extraOptions = CaseInsensitiveMap[String](Map.empty)

    val path = "C:\\Users\\smaibam\\OneDrive - Tractor Supply Co\\Desktop\\Reprocessed_Transactions.csv"
    val paths = Seq(path)

    val Ds = DataSource.apply(sparkSession,paths = paths, userSpecifiedSchema = userSpecifiedSchema,className = "csv")
    val cls = DataSource.lookupDataSource("csv", sparkSession.sessionState.conf)
    val provInstance =  cls.getConstructor().newInstance()
    // understanding resolveRelation
    // Ds.resolveRelation()
    //https://github.com/apache/spark/blob/20870c3d157ef2c154301046caa6b71cb186a4ad/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala#L228
    println(provInstance)







   // val someDF = sparkSession2.createDataFrame(spark.sparkContext.parallelize(someData),StructType(someSchema))
   // someDF.printSchema()



//    def encoderForDataType(dataType: DataType, lenient: Boolean): Any = dataType match {
//      case IntegerType => BoxedIntEncoder
//      case StringType => println("hello")
//    }

//    val replaced = CharVarcharUtils.failIfHasCharVarchar(someSchema).asInstanceOf[StructType]
//    val encoder = RowEncoder(replaced)
//    AgnosticEncoder


  }
}