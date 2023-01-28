import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

import java.io.{File, FileWriter}

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
    fw.write(args(0))
    fw.flush()
    fw.close()


    val df = spark.read.option("header", "false")
      .csv("C:\\progettoBigData\\progettoBigData\\tmp\\recensione")
      .withColumnRenamed("_c0", "text")
    //df.show()

    val nuoviDati = modelloPipeline.transform(df)

    val nuovoDatiAgain = nuoviDati.drop("label_string", "text", "tokens", "token_features")
    //nuovoDatiAgain.show()

    val rddNuoviDati = nuovoDatiAgain.rdd

    //creazione del formato LIBSVM (serve per l'input del Naive Bayesian)
    val inputNaive = rddNuoviDati.map(row => {
      val parola = row.get(0).toString
      val split1 = parola.split("\\[")
      val indici = split1(1).split("]")(0)
      val frequenze = split1(2).split("]")(0)
      val splittedIndici = indici.split(",")
      val splittedFreq = frequenze.split(",")
      var assegnazioni = ""
      var i = 0
      while (i < splittedIndici.length) {
        if (!splittedIndici(i).equals("")) {
          assegnazioni = assegnazioni + (splittedIndici(i).toInt + 1).toString + ":" + splittedFreq(i) + " "
        }
        i = i + 1
      }
      0 + " " + assegnazioni + lastValue.toString + ":0.0" //assegno una label di prova (non serve), inoltre assegno anche l'ultima colonna con valore 0
    })


    inputNaive.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\tmp\\tmpresult")

    val modelloBayes = NaiveBayesModel.load(context, "C:\\progettoBigData\\progettoBigData\\models\\bayesModel")

    val importedInput = MLUtils.loadLibSVMFile(context, "C:\\progettoBigData\\progettoBigData\\tmp\\tmpresult\\part-00000")

    val valorePredetto = importedInput.map(p => {
      (modelloBayes.predict(p.features), modelloBayes.predictProbabilities(p.features))
    })

    valorePredetto.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")

    //eliminazione della recensione temporanea
    print(file.delete()+"\n")
    //eliminazione formato LIBSVM temporaneo
    val index  = new File("C:\\progettoBigData\\progettoBigData\\tmp\\tmpresult")
    val entries = index.list
    for (s <- entries) {
      val currentFile = new File(index.getPath, s)
      print(currentFile.delete()+"\n")
    }
    print(index.delete()+"\n")

    //todo sistema problema punteggiatura, viene creata una colonna _c1!
    //todo prova aggiungendo delle parole che non esistono
    //todo possibile miglioramento con l'addestramento del naive bayesian
    //todo aggiusta meglio directory di servizio per il naive bayes

  }
}
