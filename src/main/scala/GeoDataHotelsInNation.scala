import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.File


/*
  autore: Nicola
  la classe implementa la query che data una nazione restituisce le coordinate di tutti gli hotel della nazione
 */
object GeoDataHotelsInNation extends Query {

  def main(args: Array[String]): Unit ={
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val converted: Array[String] = arguments.asInstanceOf[Array[String]]
    println(converted)
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("GeoDataHotelsInNation")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    //ATTENZIONE!! il path del file va sostituito con il vostro
    // absolute path del datatset (Serve per quello assoluto per le api rest)
    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")

    val result = getHotelsInNation(converted(0),file)

    result.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }

  def getHotelsInNation(nation:String, file:RDD[String]) ={
    val  onlyNation = file.filter(items=>{
      val address = items.split(",")(0)
      address.contains(nation)
    })
    val splitted = onlyNation.map(item=> item.split(","))
      .filter(item=>{!item(item.length-1).equals("NA") && !item(item.length-2).equals("NA")})
    splitted.map(item=>(item(4),item(0),item(3),item(item.length-2),item(item.length-1))).distinct()
  }
}
