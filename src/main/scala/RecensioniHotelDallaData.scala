import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.util.Calendar
object RecensioniHotelDallaData extends Query {

  /*
    autore: Nicola
    Dato un Hotel e un intero n restituisce le recensioni negli ultimi n giorni
    //TODO da implementare nel web server
   */
  def main(args: Array[String]): Unit = {
    compute(args)
  }

  override def compute(arguments: Any): Unit = {
    val args = arguments.asInstanceOf[Array[String]]
    val nation = args(0)
    val days: Int = args(1).toInt
    val spark: SparkSession = SparkSession.builder
      .appName("RecensioniHotelDallaData")
      .master("local[*]")
      .getOrCreate()
    val context: SparkContext = spark.sparkContext

    val file = context.textFile("C:\\progettoBigData\\progettoBigData\\Hotel_Reviews.csv")

    file.filter(row => {
      val costr1 = row.split(",")(4).equals(nation)
      var costr2 = false
      val split1 = row.split("\"")
      if(split1.length>1)
        costr2 = split1(2).split(",")(1).split(" ")(0).toInt < days
      costr1 && costr2
    })
      .map(row => {
        val splitted = row.split(",")
        if(splitted(6).toLowerCase().equals("no negative"))
          splitted(9)+"\n"
        else
          if(splitted(9).toLowerCase().equals("no positive"))
            splitted(6)+"\n"
          else
            splitted(6)+"\n"+splitted(9)+"\n "
      }).saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
  }

}
