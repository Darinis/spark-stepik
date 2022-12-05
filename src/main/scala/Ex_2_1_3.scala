import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

case object spark {
  lazy val ss: SparkSession = SparkSession.builder()
    .appName("2.1")
    .master("local")
    .getOrCreate

  lazy val sc: SparkContext = ss.sparkContext
}

object Ex_2_1_3 extends App {
  import org.apache.spark.sql.types._

  val restaurantSchema = StructType(Seq(
    StructField("average_cost_for_two", LongType),
    StructField("cuisines", StringType),
    StructField("deeplink", StringType),
    StructField("has_online_delivery", IntegerType),
    StructField("is_delivering_now", IntegerType),
    StructField("menu_url", StringType),
    StructField("name", StringType),
    StructField("url", StringType),
    StructField("user_rating",
      StructType(Seq(
        StructField("aggregate_rating", StringType),
        StructField("rating_color", StringType),
        StructField("rating_text", StringType),
        StructField("votes", StringType)
      )))
  ))

  val df = spark.ss.read
    .format("json")
    .schema(restaurantSchema)
    .load("src/main/resources/restaurant_ex.json")

  df.printSchema()
}