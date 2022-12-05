import org.apache.spark.sql.SparkSession

object Ex_2_1_2 extends App {
  val spark = SparkSession.builder()
    .appName("2.1.2")
    .master("local")
    .getOrCreate()

  val irisDF = spark.read
    .format("json") // проинструктировали spark reader прочитать файлв формате json
    .option("inferSchema", "true") // предоставляем спарку самому составить схему данных
    .load("src/main/resources/restaurant_ex.json") // указываем путь к файлу

  irisDF.show(10)
  irisDF.printSchema()

//  val irisArray:Array[Row]  = irisDF.take(3)
//  irisArray.foreach(println)

//  irisDF.take(3).foreach(print)
}