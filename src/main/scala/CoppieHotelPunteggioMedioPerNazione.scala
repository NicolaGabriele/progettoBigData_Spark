import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import ujson.Obj

/*
  autore: Nicola
  restituisce un json che contiene: data una nazione tutte le coppie (Nome hotel, punteggio medio)
  degli hotel di quella nazione.
  L'idea è quella di mostrare il tutto su un istogramma nel frontend
 */
object CoppieHotelPunteggioMedioPerNazione extends Query{

  def main(args:Array[String])={
    val json = compute(args)
    println()
    println(json)
    println()
  }

  override def compute(argument: Any): Obj = {
    val cast = argument.asInstanceOf[Array[String]]
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("numero hotel")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext
    val file = context.textFile("Hotel_Reviews.csv")
    val filtratiPerNazione = file.filter(item => {
      val tokens: Array[String] = item.split(",")(0).split(" ")
      tokens(tokens.length-1).equals(cast(0))
    })
    val coppie_Hotel_Punteggio = filtratiPerNazione.map(item => (item.split(",")(4), item.split(",")(3)))
    var ret = ujson.Obj(
      "values"-> ujson.Arr()
    )
    coppie_Hotel_Punteggio.distinct().collect().foreach(item => ret("values").arr.append(ujson.Obj("nome"->item._1, "punteggio"->item._2)))
    ret
  }

}
