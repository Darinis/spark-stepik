import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

//case object spark {
//  lazy val ss: SparkSession = SparkSession.builder()
//    .appName("2.2")
//    .master("local")
//    .getOrCreate
//
//  lazy val sc: SparkContext = ss.sparkContext
//}

object Ex_2_5_DFs_practice_1_teacher extends App {
  import org.apache.spark.sql.SaveMode
  import org.apache.spark.sql.functions._

  val customersDF = spark.ss.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/mall_customers.csv")
    .withColumn("Age",col("Age") + 2)


  val columns = Seq(col("Gender"), col("Age"))
  val isFemale = col("Gender") === "Female"
  val isMale = col("Gender") === "Male"
  val hasProperAge = col("Age").between(30, 35)


  def withProperAge(df: DataFrame): DataFrame =
    df.withColumn("Age", col("Age") + 2)

  def withGenderCode(df: DataFrame): DataFrame =
    df.withColumn("gender_code",
      when(isMale, 1)
        .when(isFemale, 0)
        .otherwise(-1))

  def extractCustomerGroups(df: DataFrame): DataFrame =
    df
      .filter(hasProperAge)
      .groupBy(columns: _*)
      .agg(round(
        avg("Annual Income (k$)"),
        1).as("avg_income"))
      .orderBy(columns: _*)


  val incomeDF = customersDF
    .transform(withProperAge)
    .transform(extractCustomerGroups)
    .transform(withGenderCode)

  incomeDF.printSchema()
  incomeDF.show(false)

//  incomeDF.write
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/customers")
}