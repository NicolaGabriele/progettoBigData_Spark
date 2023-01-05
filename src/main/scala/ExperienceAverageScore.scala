import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object ExperienceAverageScore extends Query {

  /*
    autore: Dave
    11) la classe implementa la query che dato un numero minimo e massimo di recensioni effettuate, restituisce
    il punteggio medio dei visitatori che hanno effettuato un numero di recensioni compreso tra il minimo e il massimo
   */

  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("numero hotel")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")
    val header = file.first()
    val filtrato = file.filter(row => row != header)

    /*
        rdd.mapPartitionsWithIndex {
          (idx, iter) => if (idx == 0) iter.drop(1) else iter
        }
         */

    val tupla = filtrato.map(item => {
      val splitted = item.split(",")
      val numReview = splitted(11)
      val score = splitted(12)
      (numReview.toInt, score.toFloat)
    })


    var num_totale = 1
    var progressivo = 0
    var fattoreDiScala = 10
    val listaRis : scala.collection.mutable.ListBuffer[(String,Long,Float)] = ListBuffer()

    //todo da aggiustare, così è troppo pesante
    //controllare meglio i risultati
    while (progressivo<200) {

      var filtraggio = tupla.filter(tupla => {
        tupla._1 >= progressivo && tupla._1 <= progressivo + fattoreDiScala
      })
      var num_totale = filtraggio.count()
      var totale_recensioni = filtraggio.map(tupla => tupla._2).reduce(_ + _)
      var resultProvv = totale_recensioni / num_totale
      listaRis.append((progressivo+"-"+(progressivo+fattoreDiScala), num_totale, resultProvv))
      progressivo = progressivo+10

    }

    val result = tupla.map(tupla => {
      listaRis.toList
    }).distinct()

    result.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")

  }


}
