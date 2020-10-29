package search_words

import library.{LocalSpark, SparkUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Try

object DataLakeBuilder {
  val spark = LocalSpark.getSparkSession()
  import spark.implicits._

  def readTextFiles(spark: SparkSession, config: Map[String, String]): DataFrame = {
    spark.read.textFile(s"${config("read_path")}/*.txt")
      .withColumn("filename",regexp_replace(
        regexp_extract(
          input_file_name, "\\d+\\-?\\_?\\d+?",0),"\\_","-")
        .cast(StringType))
  }

  def buildWriteDataFrame(df: DataFrame, config: Map[String, String]): DataFrame = {
    val byWordCount = Window.partitionBy("filename").orderBy('word_count desc)

    val stageDf = df.withColumn("word", explode(split(col("value"), " ")))
      .withColumn("word", lower(regexp_extract($"word", "^[A-Za-z]+$",0)))
      .groupBy($"word", $"filename")
      .agg(count($"word").as("word_count"))
      .filter($"word" =!= "")
      .orderBy($"filename", $"word_count")

    stageDf.withColumn("rank", rank over byWordCount)
      .orderBy($"filename",$"rank")
  }

  def buildDataLake(config: Map[String, String]): Unit = {
    val rawDf = readTextFiles(spark, config)
    val transformedDf = buildWriteDataFrame(rawDf, config)

    SparkUtils.writeToParquet(transformedDf,
      config("data_lake_path"),
      config("env"),
      config("event"))
  }

  def main(args: Array[String]): Unit = {

    val evn = Try(args(2)).toOption match {
      case Some(s) => args(2)
      case None => "dev"
    }
    val config = RunConfig.appconfig(evn)

    buildDataLake(config)
  }
}
