import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object ReviewsNumberFilter extends Query {

  /*
    autore: Dave
    9) la classe implementa la query che restituisce numero totale di recensioni, indirizzo dell'hotel,
    nome, latitudine e longitudine degli hotel
    con numero di recensioni superiore a quello specificato.
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

    val reviews = file.map(item => {
      val splitted = item.split(",")//.filter(item=>{!item(item.length-1).equals("NA") && !item(item.length-2).equals("NA")})
      val address = splitted(0)
      val score = splitted(3)
      val name = splitted(4)
      val lat = splitted.reverse(1)
      val lng = splitted.reverse(0)
      address + "," + score + "," + name + "," + lat + "," + lng
    }
    )

    val countingReviews =
      reviews.map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(word => (word._2, word._1))
        .filter(tuple => tuple._1>=cast(0).toInt)

    countingReviews.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }
}
