import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountNegative extends Query {

  /*
    autore: Dave
    la classe implementa la query che restituisce le parole più presenti nelle recensioni negative.
   */

  def main(args: Array[String]): Unit = {
    val json = compute(args)
    println(json)
  }

  override def compute(arguments: Any): Unit = {
    val spark = SparkSession.builder()
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    val context: SparkContext = spark.sparkContext

    //ATTENZIONE!! il path del file va sostituito con il vostro
    // absolute path del datatset (Serve per quello assoluto per le api rest)
    val file = context.textFile("C:\\Users\\Nicola\\progettoBigData\\proveVarieSpark\\Hotel_Reviews.csv")

    val result = wordCount(file)

    result.saveAsTextFile(".\\results\\result")

    //result.sortByKey(false).take(100).foreach(println) (Dave questa è solo di debug o va lasciata?) fai tu


  }

  def wordCount(file: RDD[String]) = {
    file.map(items => { items.split(",")(6) }) //prendo la colonna "Negative_Reviews"
      .filter(items => (!items.equals("No Negative") || !items.equals(""))) //non devo considerare tutte quelle recensioni "No Negative" o vuote
      .map(items => items.toLowerCase())
      .flatMap(items => items.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(word => (word._2, word._1))
      .sortByKey(true)
  }

}
