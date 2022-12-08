import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

//case object spark {
//  lazy val ss: SparkSession = SparkSession.builder()
//    .appName("2.2")
//    .master("local")
//    .getOrCreate
//
//  lazy val sc: SparkContext = ss.sparkContext
//}

object Ex_2_3_DFs_columns_3 extends App {
  import org.apache.spark.sql.functions._

  val bikeSharingDF = spark.ss.read
    .option("header","true")
    .csv("src/main/resources/bike_sharing.csv")

  bikeSharingDF
    .groupBy("Date")
    .agg(
       min("TEMPERATURE").as("min_temp")
      ,max("TEMPERATURE").as("max_temp"))
    .orderBy("Date")
    .show(10)
}