import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object AllFilters extends Query {

  /*
    autore: Dave
    18) la classe implementa la query che restituisce latitudine e longitudine degli hotel che rispettano il filtro
    con i seguenti parametri
    param1: nazione
    param2: avgscore
    param3: num_recensioni
   */

  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val args = arguments.asInstanceOf[Array[String]]
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("numero hotel")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")

    //controllo da fare per gli hotel che non hanno geotag
    val onlyGeoTag = file.filter(item => {
      val splitted = item.split(",")
      val lat = splitted(splitted.length - 2)
      val lng = splitted(splitted.length - 1)
      !(lat.equals("NA") || lat.equals("lat")) && !(lng.equals("NA") || lng.equals("lng"))
    })

    var result1 = onlyGeoTag

    //filtraggio per nazione
    if (args(0) != "all") {
      val filteredFile1 = onlyGeoTag.filter(item => {
        val nation = args(0).toLowerCase()
        item.split(",")(0).toLowerCase().contains(nation)
      })
      result1 = filteredFile1
    }

    var result2 = result1

    //filtraggio per avgscore
    if (args(1) != "all") {
      val filteredFile2 = result1.filter(item => {
        val punteggio = args(1).toFloat
        item.split(",")(3).toFloat >= punteggio
      })
      result2 = filteredFile2
    }

    //mapping sulle coordinate
    val mapping = result2.map(item=>{
      val splitted = item.split(",")
      (splitted(4),splitted(splitted.length-2),splitted(splitted.length-1)) //nomehotel per debug
    })

    var result3 = mapping

    //filtraggio per numerorecensioni
    if (args(2) != "all") {
      val countingReviews = mapping.map(coord => (coord, 1))
        .reduceByKey(_ + _)
        .filter(tupla => tupla._2 >= args(2).toInt)
        .map(tupla => tupla._1)

      result3 = countingReviews
    }

    val finalResult = result3.distinct()

    finalResult.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")

  }
}
