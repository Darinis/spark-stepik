import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//case object spark {
//  lazy val ss: SparkSession = SparkSession.builder()
//    .appName("2.2")
//    .master("local")
//    .getOrCreate
//
//  lazy val sc: SparkContext = ss.sparkContext
//}

object Ex_3_3_DS_transform_ex_1 extends App {
  import org.apache.spark.sql.functions._

  val channelsDF: DataFrame = spark.ss.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/channel.csv")

  case class Channel(
                      channel_name: String,
                      city: String,
                      country: String,
                      created: String,
                    )

  import spark.ss.implicits._
  val channelsDS = channelsDF.as[Channel]

  channelsDS.show(false)

  val cityNamesDF = channelsDF
    .select(
      upper(col("city")).as("city")
    )

  cityNamesDF.show()

  val cityNamesDS: Dataset[String] =
    channelsDS.map(channel => channel.city.toUpperCase)

  cityNamesDS.show()

}
