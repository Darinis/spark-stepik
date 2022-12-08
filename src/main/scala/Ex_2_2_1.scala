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

object Ex_2_2_1 extends App {
  import org.apache.spark.sql.SaveMode
  import org.apache.spark.sql.types._

  val moviesSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("show_id", StringType),
    StructField("type", StringType),
    StructField("title", StringType),
    StructField("director", StringType),
    StructField("cast", StringType),
    StructField("country", StringType),
    StructField("date_added", StringType),
    StructField("release_year",IntegerType),
    StructField("rating",StringType),
    StructField("duration",IntegerType),
    StructField("listed_in",StringType),
    StructField("description",StringType),
    StructField("year_added",IntegerType),
    StructField("month_added",DoubleType),
    StructField("season_count",IntegerType)
  ))

  val movies = spark.ss.read
    .option("header","true")
    .schema(moviesSchema)
    .option("nullValue", "n/a")
    .csv("src/main/resources/movies_on_netflix.csv")

  movies.printSchema()
  movies.write
    //.mode("overwrite") //не сработал вариант .mode(SaveMode.Overwrite) - уточните, пожалуйста, что следует сделать, для этого или это из-за IDEA?
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/file.parquet")
}