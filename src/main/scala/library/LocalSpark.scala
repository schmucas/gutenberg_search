package library

import org.apache.spark.sql.SparkSession

object LocalSpark {
  def getSparkSession(appName:String = "TestApp"): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .config("spark.master", "local")
      .getOrCreate()
  }
}
