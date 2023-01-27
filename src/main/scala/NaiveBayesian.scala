import CreateDataset.compute
import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{CountVectorizer, LabeledPoint, RegexTokenizer, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.util.MLUtils
import shapeless.ops.nat.GT.>
import org.apache.spark.sql.functions.rand

import javax.servlet.Registration.Dynamic




object NaiveBayesian extends Query {

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


    /*

    val df = spark.read.option("header", "false")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv("naive_bayesian_dataset.txt")
    .withColumnRenamed("_c0", "label_string")
    .withColumnRenamed("_c1", "text")
    df.show()

    //shuffle e selezione di righe
    val shuffledDF = df.orderBy(rand())
    val dataframe = shuffledDF.limit(5000)

    //preprocessing

    //clean data and tokenize sentences using RegexTokenizer
    val regexTokenizer = new RegexTokenizer()
    regexTokenizer.setInputCol("text")
    regexTokenizer.setOutputCol("tokens")
    regexTokenizer.setPattern("\\W+")

    val cv = new CountVectorizer() //vocabSize
    cv.setInputCol("tokens")
    cv.setOutputCol("token_features")
    cv.setMinDF(2.0) //che cos'è?

    val indexer = new StringIndexer() //(inputCol = "label_string", outputCol = "label")
    indexer.setInputCol("label_string")
    indexer.setOutputCol("label")

    val vecAssembler = new VectorAssembler() //(inputCols =['token_features'], outputCol = "features"
    val arr = Array[String]{"token_features"}
    vecAssembler.setInputCols(arr)
    vecAssembler.setOutputCol("features")

    val stages = Array(regexTokenizer,cv,indexer,vecAssembler)

    val pipeline = new Pipeline()
    pipeline.setStages(stages)
    val data = pipeline.fit(dataframe).transform(dataframe)

    val nuovoData = data.drop("label_string","text","tokens","token_features")
    nuovoData.show()

    val rddData = nuovoData.rdd

    //creazione del formato LIBSVM (serve per l'input del Naive Bayesian)
    val nuovoDataset = rddData.map(row => {
      val parola = row.get(1).toString
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
      row.get(0) + " " + assegnazioni
    })

    nuovoDataset.saveAsTextFile("C:\\progettoBigData\\progettoBigData\\results\\result")
     */


    val importedData = MLUtils.loadLibSVMFile(context, "C:\\progettoBigData\\progettoBigData\\nuovoDataset")

    val Array(training, test) = importedData.randomSplit(Array(0.7, 0.3),2023)

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    print("accuratezza: ")
    print(accuracy.toString)
    print("\n")


  }

}
