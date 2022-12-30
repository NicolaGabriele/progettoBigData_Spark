
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import shapeless.syntax.std.tuple.productTupleOps

/*
  autore: Matto
  Restituisce tutte le coppie (Nome Hotel, Numero di recensioni positive)
  L'idea è di creare una sorta di grafica Youtube Style simil Like-Dislike
 */

object CoppieHotel_PosReviews extends Query {

  def main(args:Array[String]): Unit = {
    compute(args)
  }//main

  override def compute(argument: Any): Unit={
    //val cast = argument.asInstanceOf[Array[String]]
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("CoppieHotel_NumRevPos")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext


    //absolute path che servirà poi per le Api Rest
    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")

    //Nelle recensioni negative da conteggiare, ovviamente, non considereremo le recensioni = "No Negative" e vuote
    file.map(items=>(
      items.split(",")(4),
      items.split(",")(9)
    )).filter(
      items=>{
        (!(items._2.toLowerCase().equals("no positive")) && !(items._2.length==0))  //Filtriamo eliminando le recensioni No Positive e Vuote
      })
      .map(items=>(items._1,1))
      .reduceByKey(_+_)
      .map(items=> (items._2,items._1))
      .sortByKey(true)
      .saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }//compute

}//CoppieHotel_NegReviews
