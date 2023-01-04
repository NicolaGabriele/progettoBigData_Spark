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

    //todo si potrebbe migliorare la query scartando tutte le nazionalità con meno di 50 o 100 recensioni

    //todo si può fare meglio?
    val nat_total_score = filtrato.map(item => {
      val splitted = item.split(",")
      val nationality = splitted(5)
      val score = splitted(12)
      (nationality,score.toFloat)
    }).reduceByKey(_+_)

    val nat_num_reviews = filtrato.map(item => {
      val splitted = item.split(",")
      val nationality = splitted(5)
      (nationality, 1)
    }).reduceByKey(_+_).collect()


    val result = nat_total_score.map(tuple1 => {
        val num_tot = nat_num_reviews.find(x => x._1.equals(tuple1._1)).getOrElse(("Pippo",1))
        (tuple1._1,tuple1._2,num_tot._2)
    }).map(triple => (triple._1,(triple._2/triple._3)))


    result.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }
}
