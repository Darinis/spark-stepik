import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object Ex_2_1_1 extends App {
  val spark = SparkSession.builder()
    .appName("2.1.1")
    .master("local")
    .getOrCreate()

  val data = Seq(
    Row("s9FH4rDMvds", "2020-08-11T22:21:49Z", "UCGfBwrCoi9ZJjKiUK8MmJNw", "2020-08-12T00:00:00Z"),
    Row("kZxn-0uoqV8", "2020-08-11T14:00:21Z", "UCGFNp4Pialo9wjT9Bo8wECA", "2020-08-12T00:00:00Z"),
    Row("QHpU9xLX3nU", "2020-08-10T16:32:12Z", "UCAuvouPCYSOufWtv8qbe6wA", "2020-08-12T00:00:00Z")
  )

  import org.apache.spark.sql.types._

  val schema = List(
    StructField("videoId", StringType, true),
    StructField("publishedAt", StringType, true),
    StructField("channelId", StringType, true),
    StructField("trendingDate", StringType, true)
  )

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    StructType(schema)
  )
  df.show()
  df.printSchema()
}