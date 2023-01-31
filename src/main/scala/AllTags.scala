import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object AllTags extends Query {

  /*
    autore: Dave
    13) la classe implementa la query che dato il nome di un hotel, restituire tutti i tag distinti
   */

  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val cast = arguments.asInstanceOf[Array[String]] //prendo il nome dell'hotel
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("numero hotel")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")


    //prendo solo le recensioni sull'hotel che mi interessano
    val hotelInterested = file.filter(item => {
      item.split(",")(4).equals(cast(0))
    })

    //prendo la lista dei tag distinti
    val allTags = hotelInterested.map(item => {
      val tuttiTag1 = item.split("]")(0)
      val tuttiTag2 = tuttiTag1.split("\\[")(1)
      val splitted = tuttiTag2.split("'")
      var i = 1
      var ret = ""
      while (i<splitted.length){
        ret = ret + splitted(i) + ","
        i = i+2
      }
      ret
    }).flatMap(item => {item.split(",")}).distinct()

    /*
    val allHotelsName = file.map(item => {
      "'"+item.split(",")(4)+"'"+","
    }).distinct()
     */

    allTags.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }
}
