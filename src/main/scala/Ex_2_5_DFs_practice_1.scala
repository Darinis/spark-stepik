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

object Ex_2_5_DFs_practice_1 extends App {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SaveMode

  val customersDF = spark.ss.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/mall_customers.csv")
    .withColumn("Age",col("Age") + 2)

  customersDF.printSchema()

  val hasProperAge = col("Age").between(30, 35)
  val isMale = col("Gender") === "Male"
  val columns = Seq(col("Gender"), col("Age"))

  val incomeDF = customersDF
    .filter(hasProperAge)
    .groupBy(columns: _*)
    .agg(
      round(avg("Annual Income (k$)"),1).as("avg_income"))
    .orderBy(columns: _*)
    .withColumn("gender_code",
      when(isMale,1).otherwise(0)
    )

  incomeDF.printSchema()
  incomeDF.show(false)

  incomeDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/customers")
}