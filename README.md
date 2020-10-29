# PROJECT GUTENBERG

## Setup

### Prerequisites
Install Scala and Spark:
https://medium.com/beeranddiapers/installing-apache-spark-on-mac-os-ce416007d79f

The dependencies are set up to run with Scala 2.12 and Spark 3.0.1.
If you have a different version installed make sure to update build.sbt

### Set up configurations
Before running application, two variables have to be defined in library.Constants
* "searchWordReadPath" => Define read path for source .txt files (code will only accept .txt files)
* "dataLakePath" => Define path for data lake (Code will write parquet files)

### Build Jar
run
```shell script
sbt package
```

Once compiled, save jar path in text editor (can be found in target folder)

### Run application
open terminal and start spark
```shell script
spark-shell
```
then add jar
```shell script
:require [this is a path].jar
```
if successful. shell will print something like
"Added [this is a path].jar"

Note that the application is set up to run in different environments:
dev, staging and prod. It is optional to run in a specific environment 
however if not defined code will run in dev

now, let's build the data lake from the raw .txt files
(note that the argument in the main function is optional)
```shell script
search_words.DataLakeBuilder.main(Array("dev"))
```
(this might take a while depending on the amount of files read)

once data lake is built. We can perform the searches
(again, 3rd argument is optional)
```shell script
search_words.SearchWords.main(Array("1998","10","dev"))
```
or
```shell script
search_words.SearchWords.main(Array("fish","10","dev"))
```
(the word search is not case sensitive)

## Notes

In order to scale up and run on a distributed system jar could be installed on AWS EMR or Databricks. I recommend using EMR due to the price advantage (Databricks is double the cost) and use Databricks for development and debugging.
