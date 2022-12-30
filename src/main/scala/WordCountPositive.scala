import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountPositive extends Query {

  /*
     autore: Dave
     2) la classe implementa la query che restituisce le parole più presenti nelle recensioni positive
    */

  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    val context: SparkContext = spark.sparkContext

    //ATTENZIONE!! il path del file va sostituito con il vostro
    // absolute path del datatset (Serve per quello assoluto per le api rest)
    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")

    val result = wordCount(file)

    result.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")

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
