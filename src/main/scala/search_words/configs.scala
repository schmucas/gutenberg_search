package search_words

import library.Constants

case class SearchWordsConfig(
                 configs: Map[String,String] = Map(
                   "read_path" -> Constants.searchWordReadPath,
                   "data_lake_path" -> Constants.dataLakePath,
                   "event" -> "gutenberg",
                   "partition_column" -> "filename",
                   "env" -> "dev"
                 )
)

object SearchWordsConfigDev extends SearchWordsConfig ()

object SearchWordsConfigStaging extends SearchWordsConfig (
  configs = Map(
    "env" -> "staging"
  )
)

object SearchWordsConfigProd extends SearchWordsConfig (
  configs = Map(
    "env" -> "prod"
  )
)

object RunConfig {
  val appconfig = Map (
    "dev" -> SearchWordsConfigDev.configs,
    "staging" -> SearchWordsConfigStaging.configs,
    "prod" -> SearchWordsConfigProd.configs
  )
}
