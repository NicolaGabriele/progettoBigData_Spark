import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StringIndexer, VectorAssembler}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.rand

object SupportBayesian extends Query {

  /*
    autore: Dave
    implementazione naive bayesian
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



    //preparazione del dataset

    //estrazione dataframe
    val df = spark.read.option("header", "false")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv("naive_bayesian_dataset.txt")
    .withColumnRenamed("_c0", "label_string")
    .withColumnRenamed("_c1", "text")

    df.show()

    //shuffle e selezione di righe
    val shuffledDF = df.orderBy(rand())
    val dataframe = shuffledDF.limit(100000)


    //preprocessing

    val regexTokenizer = new RegexTokenizer()
    regexTokenizer.setInputCol("text")
    regexTokenizer.setOutputCol("tokens")
    regexTokenizer.setPattern("\\W+")

    val cv = new CountVectorizer()
    cv.setInputCol("tokens")
    cv.setOutputCol("token_features")
    cv.setMinDF(2.0)

    val indexer = new StringIndexer()
    indexer.setInputCol("label_string")
    indexer.setOutputCol("label")

    val vecAssembler = new VectorAssembler()
    val arr = Array[String]{"token_features"}
    vecAssembler.setInputCols(arr)
    vecAssembler.setOutputCol("features")

    //costruzione della pipeline che serve per le nuove recensioni (regexTokenizer,countVectorizer,vectorAssembler)
    val stages = Array(regexTokenizer,cv,vecAssembler)
    val pipeline = new Pipeline()
    pipeline.setStages(stages)
    val modelPipeline = pipeline.fit(dataframe)
    val data = modelPipeline.transform(dataframe)

    modelPipeline.save("C:\\progettoBigData\\progettoBigData\\models\\pipelineModel")


    //completiamo con l'indexer, per avere un dataframe con label (utile per l'addestramento)
    val finalData = indexer.fit(data).transform(data)
    finalData.show()

    val nuovoData = finalData.drop("label_string","text","tokens","token_features")
    nuovoData.show()


    val rddData = nuovoData.rdd

    //creazione del formato LIBSVM (serve per l'input del Naive Bayesian)
    val nuovoDataset = rddData.map(row => {
      val parola = row.get(0).toString
      val split1 = parola.split("\\[")
      val indici = split1(1).split("]")(0)
      val frequenze = split1(2).split("]")(0)
      val splittedIndici = indici.split(",")
      val splittedFreq = frequenze.split(",")
      var assegnazioni = ""
      var i = 0
      while(i<splittedIndici.length){
        if (!splittedIndici(i).equals("")) {
          assegnazioni = assegnazioni + (splittedIndici(i).toInt + 1).toString + ":" + splittedFreq(i) + " "
        }
        i = i+1
      }
      row.get(1) + " " + assegnazioni
    })

    nuovoDataset.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")



    val importedData = MLUtils.loadLibSVMFile(context, "C:\\progettoBigData\\progettoBigData\\nuovoDataset")

    val Array(training, test) = importedData.randomSplit(Array(0.7, 0.3),2023)

    val modelBayes = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (modelBayes.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()


    //accuratezza del 92%
    print("accuratezza: ")
    print(accuracy.toString)
    print("\n")


    //salvataggio del modello addestrato
    modelBayes.save(context,"C:\\progettoBigData\\progettoBigData\\models\\bayesModel")

  }

}
