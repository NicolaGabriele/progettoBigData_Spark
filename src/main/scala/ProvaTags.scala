import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object ProvaTags extends Query {

  /*
    autore: Dave
    13) la classe implementa la query che dato il nome dell'hotel,
     restituisce la lista di triple:  (tag, del punteggio medio fornito su quel tag, numero totale di recensioni con quel tag)
     OSS: escludiamo i tag dei "submitted from a mobile device"
   */

  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val cast = arguments.asInstanceOf[Array[String]] //prendo il nome dell'hotel
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

    //prendo la lista dei tag
    val allTags = hotelInterested.map(item => {
      val tuttiTag1 = item.split("]")(0)
      val tuttiTag2 = tuttiTag1.split("\\[")(1)
      tuttiTag2
      /*
      val tags0 = item.split(",")(13)
      val tags1 = item.split(",")(14)
      val tags2 = item.split(",")(15)
      var tags3 = item.split(",")(16)
      tags0.split("'")(1)+","+tags1.split("'")(1)+","+tags2.split("'")(1)+"," //+tags3.split("'")(1)
       */
      //todo trovare un modo per prendere tags3
    }).flatMap(item => {item.split(",")}).distinct()

    allTags.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }
}
