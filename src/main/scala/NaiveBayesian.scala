import CreateDataset.compute
import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import shapeless.ops.nat.GT.>

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

     val df = spark.read.option("header", "false")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv("naive_bayesian_dataset.txt")
    .withColumnRenamed("_c0", "label_string")
    .withColumnRenamed("_c1", "text")
    df.show()

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
    val data = pipeline.fit(df).transform(df)

    data.show()

    //todo continua

  }

}
