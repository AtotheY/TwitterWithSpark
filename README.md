#Twitter With Spark  
Working on implementing from this source  
https://databricks.gitbooks.io/databricks-spark-reference-applications/content/twitter_classifier/collect.html  
  
To run this, you either need a twitter.txt file with your oauth info in it placed in /src/, or use Intellij to set program arguments.
If using Intellij, make sure your program arguments are clean (i.e not consumerKey=********, just ********). Also, this project uses some outdated
Spark streaming libraries due to compatibility issues with Spark, Spark Streaming, and Scala. Using Scala sdk v2.10.5, Spark-core 2.10.1, Spark streaming 2.10.1
