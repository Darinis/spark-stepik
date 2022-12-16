import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

//case object spark {
//  lazy val ss: SparkSession = SparkSession.builder()
//    .appName("2.2")
//    .master("local")
//    .getOrCreate
//
//  lazy val sc: SparkContext = ss.sparkContext
//}

object Ex_3_2_DS_null_1 extends App {
  import org.apache.spark.sql.functions._
  import spark.ss.implicits._

  case class Shoes(
                    item_category: String,
                    item_name: String,
                    item_after_discount: String,
                    item_price: String,
                    percentage_solds: Int,
                    item_rating: Int,
                    item_shipping: String,
                    buyer_gender: String
                  )

  val shoesDF = spark.ss.read
    .option("header","true")
    .option("inferSchema","true")
    .csv("src/main/resources/athletic_shoes.csv")

  shoesDF.show(false)

  val columns = Seq("item_category","item_name")

  def replaceNull(colName: String, column: Column): Column =
    coalesce(col(colName), column).as(colName)

  val df: DataFrame = shoesDF
    .na.drop(columns)
    .select(
      col("item_category"),
      col("item_name"),
      replaceNull("item_after_discount", col("item_price")),
      replaceNull("item_rating", lit(0)),
      replaceNull("percentage_solds", lit(-1)),
      replaceNull("buyer_gender", lit("unknown")),
      replaceNull("item_price", lit("n/a")),
      replaceNull("item_shipping", lit("n/a"))
    )

  val shoesDS = df.as[Shoes]
  shoesDS.show(false)
}