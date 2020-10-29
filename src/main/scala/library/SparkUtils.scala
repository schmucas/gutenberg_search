package library

import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode._

object SparkUtils {

  def readFromParquet(spark: SparkSession,
                      dataLakePath: String,
                      env: String,
                      event: String,
                      partitionColumn: String = "",
                      partitionFile: String = ""): DataFrame = {
    val partition = if (partitionFile != "") s"${partitionColumn}=${partitionFile}/" else ""
    spark.read.format("parquet")
      .option("basePath", s"${dataLakePath}/${env}/${event}")
      .load(s"${dataLakePath}/${env}//${event}/${partition}")
  }

  def writeToParquet(df: DataFrame,
                     dataLakePath: String,
                     env: String,
                     event: String,
                     saveMode: SaveMode=Overwrite): Unit = {
    df.write
      .partitionBy("filename")
      .mode(saveMode)
      .parquet(s"${dataLakePath}/${env}/${event}/")
  }

}
