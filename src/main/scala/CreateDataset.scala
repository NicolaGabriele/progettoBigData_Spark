
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object CreateDataset extends Query {

  /*
    autore: Dave
    classe di servizio: crea il dataset utile per il naive bayesian
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


    //prendo solo le recensioni sull'hotel che mi interessano
    val recensioni_negative = file.map(item => {
      "Negative , "+item.split(",")(6)
    }).filter(item => {
      !item.split(",")(1).contains("No Negative")
    })

    val recensioni_positive = file.map(item => {
      "Positive , " + item.split(",")(9)
    }).filter(item => {
      !item.split(",")(1).contains("No Positive")
    })

    val newDataset = recensioni_negative.union(recensioni_positive)

    newDataset.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }
}
