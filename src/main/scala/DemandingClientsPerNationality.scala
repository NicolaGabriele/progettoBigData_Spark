
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DemandingClientsPerNationality extends Query {

  /*
    autore: Dave
    10) la classe implementa la query che restituisce la coppia (Nazionalità visitatori,Punteggio Medio dei visitatori)
   */

  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("numero hotel")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")
    val header = file.first()
    val filtrato = file.filter(row => row != header)

    /*
    rdd.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
     */

    //todo passaggio hotel?
    //todo passaggio nazione?

    val nat_total_score = filtrato.map(item => {
      val splitted = item.split(",")
      val nationality = splitted(5)
      val score = splitted(12)
      (nationality,score.toDouble)
    }).reduceByKey(_+_)

    val nat_num_reviews = filtrato.map(item => {
      val splitted = item.split(",")
      val nationality = splitted(5)
      (nationality, 1.0)
    }).reduceByKey(_+_)

    //filtraggio con 100 recensioni
    val nat_num_filter = nat_num_reviews.filter(item => item._2>=100)

    val result = nat_total_score.join(nat_num_filter)
      .map(item => (item._1,item._2._1/item._2._2))
      .sortBy(item => item._2, true)


    result.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }
}
