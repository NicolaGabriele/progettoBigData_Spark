import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AverageScoreFilter extends Query {

  /*
    autore: Dave
    8) la classe implementa la query che restituisce indirizzo dell'hotel, nome, latitudine e longitudine degli hotel
    con punteggio superiore a quello specificato.
   */

  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val cast = arguments.asInstanceOf[Array[String]]
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("numero hotel")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    //ATTENZIONE!! il path del file va sostituito con il vostro
    // absolute path del datatset (Serve per quello assoluto per le api rest)
    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")

    //aggiusta meglio il filtraggio sull'header todo
    val filteredByAvScore =
      file.filter(item => {
        !item.split(",")(3).equals("Average_Score") && {
          val score: Float = item.split(",")(3).toFloat
          score >= cast(0).toFloat
        }
    })

    val uniqueHotels = filteredByAvScore.map(item => {
      val splitted = item.split(",")
      val address = splitted(0)
      val score = splitted(3)
      val name = splitted(4)
      val lat = splitted.reverse(1)
      val lng = splitted.reverse(0)
      address + "," + score + "," + name + "," + lat + "," + lng
    }
    ).distinct()

    uniqueHotels.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }
}
