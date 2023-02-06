import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountNegNat extends Query {

  /*
    autore: Dave
    1*) la classe implementa la query che restituisce le parole più presenti nelle recensioni negative.
   */

  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val args = arguments.asInstanceOf[Array[String]]
    val spark = SparkSession.builder()
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    val context: SparkContext = spark.sparkContext

    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")
    //filtraggio intermedio sulla nazionalità

    val filteredFile = file.filter(item => {
      val nomeNazionalita = args(0).toLowerCase()
      item.split(",")(5).toLowerCase().contains(nomeNazionalita)
    })

    val result = wordCount(filteredFile)

    result.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")

  }

  val stopWords = Array("","i","me","my","we","you","it","its","what","which","with","as","they","from",
    "this","that","am","is","are","was","were","be","have","has","had","a","an","the","and","but","nothing",
    "or","of","at","to","on","off", "no", "not", "so", "s", "t","negative","positive","in","for","there","very",
    "our","would","could","when","all","too","one","only","bit","out","didn","if","more","been","us","get","up","hotel")

  def isStopWord(input: String, lista: Array[String]): Boolean = {
    for (w <- lista){
      if (input.equals(w)) return true
    }
    false
  }
  def wordCount(file: RDD[String]) = {
    file.map(items => { items.split(",")(6) }) //prendo la colonna "Negative_Reviews"
      .filter(items => (!items.equals("No Negative") || !items.equals(""))) //non devo considerare tutte quelle recensioni "No Negative" o vuote
      .map(items => items.toLowerCase())
      .flatMap(items => items.split(" "))
      .filter(item => {
        !isStopWord(item,stopWords)
      })
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(word => (word._2, word._1))
      .sortByKey(true)
  }

}
