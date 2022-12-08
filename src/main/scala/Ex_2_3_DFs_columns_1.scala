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

//различные методы для выбора одной и той же колонки
//  bikeSharingDF.select(
//    sharingDF.col("Date"),
//    col("Date"),
//    column("Date"),
//    Symbol("Date"),
//    $"Date",
//    expr("Date")
//  )

object Ex_2_3_DFs_columns_1 extends App {
  import org.apache.spark.sql.functions._

  val bikeSharingDF = spark.ss.read
    .option("header","true")
    .csv("src/main/resources/bike_sharing.csv")

  val DFselect = bikeSharingDF.select(col("Hour"),col("TEMPERATURE"),col("HUMIDITY"),col("WIND_SPEED"))

  DFselect.printSchema()
  DFselect.show(3)
}