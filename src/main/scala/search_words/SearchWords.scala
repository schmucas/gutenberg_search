package search_words

import library.{LocalSpark, SparkUtils}
import org.apache.spark.sql.DataFrame
import scala.util.Try


object SearchWords {
  val spark = LocalSpark.getSparkSession()
  import spark.implicits._

  def getTopWordsForFile(config: Map[String,String], fileId: String, limit: Integer): DataFrame = {
    SparkUtils.readFromParquet(spark,
      config("data_lake_path"),
      config("env"),
      config("event"),
      config("partition_column"),
      fileId)
      .filter($"rank" <= limit)
      .orderBy($"rank".asc)
      .select($"word",$"word_count",$"filename")
  }

  def getTopFilesByWord(config: Map[String,String], word: String, limit: Integer): DataFrame = {
    SparkUtils.readFromParquet(spark,
      config("data_lake_path"),
      config("env"),
      config("event"),
      config("partition_column"))
      .filter($"word" === word.toLowerCase() && $"rank" === 1)
      .limit(limit)
      .orderBy($"word_count".desc)
      .select($"word",$"word_count",$"filename")
  }

  def main(args: Array[String]): Unit = {

    val evn = Try(args(2)).toOption match {
      case Some(s) => args(2)
      case None => "dev"
    }
    val config = RunConfig.appconfig(evn)

      Try(args(0).take(1).toInt).toOption match {
        case Some(i) => getTopWordsForFile(config, args(0), args(1).toInt).show(args(1).toInt)
        case None => getTopFilesByWord(config, args(0), args(1).toInt).show(args(1).toInt)
      }
    }
}
