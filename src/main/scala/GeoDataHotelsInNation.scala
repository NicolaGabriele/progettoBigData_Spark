import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/*
  autore: Nicola
  la classe implementa la query che data una nazione restituisce le coordinate di tutti gli hotel della nazione
 */
object GeoDataHotelsInNation extends Query {

  def main(args: Array[String]): Unit ={
    val json = compute(args)
    println(json)
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
    val file = context.textFile("C:\\Users\\Nicola\\progettoBigData\\proveVarieSpark\\Hotel_Reviews.csv")

    val result = getHotelsInNation(converted(0),file)

    result.saveAsTextFile(".\\results\\result")
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
