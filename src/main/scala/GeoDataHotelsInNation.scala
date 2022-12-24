import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
  autore: Nicola
  la classe implementa la query che data una nazione restituisce le coordinate di tutti gli hotel della nazione in formato json.
  il json va inviato al front-end per la visualizzazione su mappa
 */
object GeoDataHotelsInNation {

  def main(args: Array[String]): Unit ={
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("prova1")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    val file = context.textFile("Hotel_Reviews.csv")

    val result = getHotelsInNation(args[0].toString(),file)

    var json = ujson.Obj(
      "values"->ujson.Arr()
    )
    result.collect().foreach(item => json("values").arr.append(ujson.Obj("latitudine"->item._1,"longitudine"->item._2)))

    //il json contiene le coordinate di ciascun hotel della nazione scelta

  print()

  }

  def getHotelsInNation(nation:String, file:RDD[String]) ={
    val  onlyNation = file.filter(items=>{
      val address = items.split(",")(0).split(" ")
      val stringa = address(address.length-1)
      stringa.equals(nation)
    })
    val splitted = onlyNation.map(item=> item.split(","))
    splitted.map(item=>(item(item.length-2),item(item.length-1))).distinct()
  }

}
