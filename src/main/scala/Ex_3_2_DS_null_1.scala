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

object Ex_3_2_DS_null_1 extends App {
  import org.apache.spark.sql.functions._

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

  import spark.ss.implicits._

  val shoesDS = shoesDF.as[Shoes]

//  shoesDS.printSchema()

  val columns = Seq("item_category","item_name")

  val interDS = shoesDS
    .na.drop(columns)
    .select(
      col("item_category"),
      col("item_name"),
      coalesce(col("item_after_discount"),col("item_price"),lit("n/a")).as("item_after_discount"),
      col("item_price"),
      col("percentage_solds"),
      coalesce(col("item_rating"),lit(0)).as("item_rating"),
      col("item_shipping"),
      coalesce(col("buyer_gender"),lit("unknown").as("buyer_gender"))
    )

  val DS = interDS
    .na
    .fill("n/a", List("item_price", "item_shipping"))

}

// Спасибо большое за рецензии!
// Мне кажется мой код выгдялит "перегруженным", есть ли возможность здесь сделать как вы предлагали альтернативу с "transform"?
// Или такой вариант приемлем?
// Спасибо!