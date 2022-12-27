import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import ujson.Obj

object WordCountPositive extends Query {

  /*
     autore: Dave
     la classe implementa la query che restituisce le parole più presenti nelle recensioni positive in formato json.
     il json va inviato al front-end per la visualizzazione grafica
    */

  def main(args: Array[String]): Unit = {
    val json = compute(args)
    println(json)
  }

  override def compute(arguments: Any): Obj = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    val context: SparkContext = spark.sparkContext

    val file = context.textFile("Hotel_Reviews.csv")

    val result = wordCount(file)

    result.sortByKey(false).take(100).foreach(println)

    var json = ujson.Obj(
      "values" -> ujson.Arr()
    )

    //result.collect().foreach(item => json("values").arr.append(ujson.Obj("counter" -> item._1, "word" -> item._2)))
    json

  }

  def wordCount(file: RDD[String]) = {
    file.map(items => {
      items.split(",")(9)
    }) //prendo la colonna "Positive_Reviews"
      .filter(items => (!items.equals("No Positive") || !items.equals(""))) //non devo considerare tutte quelle recensioni "No Positive" o vuote
      .map(items => items.toLowerCase())
      .flatMap(items => items.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(word => (word._2, word._1))
      .sortByKey(true)
  }

}
