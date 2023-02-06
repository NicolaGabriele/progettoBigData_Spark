import org.apache.avro.io.Encoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter

object TimeScoreEvolutionMonth extends Query {

  /*
    autore: Dave
    12*) la classe implementa la query che dato il nome dell'hotel restituisce data recensione e punteggio fornito dall'utente
    NOTA: I risultati devono essere ordinati cronologicamente
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

    //prendo solo le recensioni sull'hotel che mi interessano, mi prendo solo la data e il punteggio dell'utente
    val dataPunteggi = file.filter(item => {
      item.split(",")(4).toLowerCase().equals(cast(0).toLowerCase())
    })
      .map(item => {
        val splitted = item.split(",")
        val data = splitted(2)
        val punteggio = splitted(12)
        data + "," + punteggio
      })
      .map(item => {
        val data = item.split(",")(0).split(("/"))
        val giorno = "1" //placeholder, fisso il giorno uguale per tutti
        val mese = data(0)
        val anno = data(2)
        (mese + "/" + giorno + "/" + anno, item.split(",")(1).toDouble)
      })

    val dataPunteggioTotale = dataPunteggi.reduceByKey(_ + _)

    val dataRecensioniTotali = dataPunteggi.map(item => (item._1,1.0))
      .reduceByKey(_ + _)

    val result = dataPunteggioTotale.join(dataRecensioniTotali)
      .map(item => (item._1,item._2._1/item._2._2))
      .sortBy(item => {
        val data = item._1.split(",")(0)
        val mese = data.split("/")(0).toInt
        val giorno = data.split("/")(1).toInt
        val anno = data.split("/")(2).toInt
        DateTime.parse(anno+"-"+mese+"-"+giorno+"T01:20").toCalendar(null)
      })

    result.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")

  }
}