import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/*
  autore: Nicola
  restituisce, data una nazione, tutte le coppie (Nome hotel, punteggio medio)
  degli hotel di quella nazione.
  L'idea è quella di mostrare il tutto su un istogramma nel frontend
 */
object CoppieHotelPunteggioMedioPerNazione extends Query{

  def main(args:Array[String])={
    compute(args)
  }

  override def compute(argument: Any): Unit = {
    val cast = argument.asInstanceOf[Array[String]]
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("numero hotel")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    //ATTENZIONE!! il path del file va sostituito con il vostro
    // absolute path del datatset (Serve per quello assoluto per le api rest)
    val file = context.textFile("C:\\Users\\Nicola\\progettoBigData\\proveVarieSpark\\Hotel_Reviews.csv")

    val filtratiPerNazione = file.filter(item => {
      val tokens: Array[String] = item.split(",")(0).split(" ")
      tokens(tokens.length-1).equals(cast(0))
    })
    val coppie_Hotel_Punteggio = filtratiPerNazione.map(item => (item.split(",")(4), item.split(",")(3)))
    coppie_Hotel_Punteggio.saveAsTextFile(".\\results\\result")

  }

}
