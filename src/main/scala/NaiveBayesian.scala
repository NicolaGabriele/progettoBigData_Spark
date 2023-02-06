import org.apache.commons.text.StringTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

import java.io.{File, FileWriter}
import java.lang.NumberFormatException

object NaiveBayesian extends Query{
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


    //prova predizioni richiamando i modelli salvati
    val modelloPipeline = PipelineModel.load("C:\\progettoBigData\\progettoBigData\\models\\pipelineModel")
    val lastValue = 13384
    val recensioni_positive = 0
    val recensioni_negative = 1

    //creazione file temporaneo della recensione
    val file = new File("C:\\progettoBigData\\progettoBigData\\tmp\\recensione")
    val fw = new FileWriter(file)

    //rimozione punteggiatura
    val splitted = args(0).split(",|;|\\.|:|\\(|\\)|\\[|]|!|\\?|")
    var i = 0
    var res = ""
    while (i<splitted.length) {
      res = res + splitted(i)
      i = i+1
    }

    fw.write(res)
    fw.flush()
    fw.close()


    val df = spark.read.option("header", "false")
      .csv("C:\\progettoBigData\\progettoBigData\\tmp\\recensione")
      .withColumnRenamed("_c0", "text")
    df.show()

    val nuoviDati = modelloPipeline.transform(df)

    val nuovoDatiAgain = nuoviDati.drop("label_string", "text", "tokens", "token_features")
    nuovoDatiAgain.show()


    val rddNuoviDati = nuovoDatiAgain.rdd

    val labeledData = rddNuoviDati.map(row => {
      val parola = row.get(0).toString
      val split1 = parola.split("\\[")
      val indici = split1(1).split("]")(0)
      val frequenze = split1(2).split("]")(0)
      val splittedIndici = indici.split(",")
      val splittedFreq = frequenze.split(",")
      var i = 0
      val arr = Array.fill(13384)(0.0)
      while (i < splittedIndici.length) {
          if (!splittedIndici(i).equals("")) {
            arr(splittedIndici(i).toInt) = splittedFreq(i).toDouble
          }
        i = i + 1
      }
        LabeledPoint(0, Vectors.dense(arr))
    })

    val modelloBayes = NaiveBayesModel.load(context, "C:\\progettoBigData\\progettoBigData\\models\\bayesModel")

    val valorePredetto = labeledData.map(p => {
      (modelloBayes.predict(p.features), modelloBayes.predictProbabilities(p.features))
    })

    valorePredetto.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")

    //eliminazione della recensione temporanea
    print(file.delete()+"\n")

    //todo possibile miglioramento con l'addestramento del naive bayesian

  }
}
