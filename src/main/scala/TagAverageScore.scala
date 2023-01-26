import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.{File, FileWriter}
import java.nio.file.{Files}
import  java.nio.file.Path;

object TagAverageScore extends Query {

  /*
  autore: Dave
  15) la classe implementa la query che dato il nome dell'hotel e un tag,
   restituisce la tripla:  (tag, punteggio medio fornito da recensioni con tag, numero totale di recensioni con quel tag)
   OSS: escludiamo i tag dei "submitted from a mobile device"
 */

  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val cast = arguments.asInstanceOf[Array[String]] //prendo il nome dell'hotel e il tag
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("numero hotel")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")


    //prendo solo le recensioni sull'hotel che mi interessano
    val hotelInterested = file.filter(item => {
      item.split(",")(4).equals(cast(0))
    })

    //prendo solo le recensioni con quel tag
    val allTags = hotelInterested.filter(item => {
      val tuttiTag1 = item.split("]")(0)
      val tuttiTag2 = tuttiTag1.split("\\[")(1)
      tuttiTag2.contains(cast(1))
    })

    val numeroRecensioni = allTags.count()

    val totaleRecensioni = allTags.map(item => {
      item.split(",")(12).toFloat
    }).reduce(_+_)

    val punteggioMedio = totaleRecensioni/numeroRecensioni

    val tripla = (cast(1),punteggioMedio,numeroRecensioni)

    val result = allTags.map(item => tripla).distinct()

    result.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")

  }

}