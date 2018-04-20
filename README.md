Maven Project for SANSA using Spark
=============================

This is a [Maven](https://maven.apache.org/)  generate a [SANSA](https://github.com/SANSA-Stack)-KGML project using [Apache Spark](http://spark.apache.org/).

Installation:
----------

```
git clone https://github.com/white-hat-of-github/SANSA-KGML.git
cd SANSA-KGML

mvn clean package
````

### Installing wordNet database
##### In the root directory of the project creat a folder with name "link" then run:
```
sbt download-database
```


## To be able to run the project in Spark Cluster mode(other than local or stand alone) 
Upload the config directory to HDFS

# Execution 
### Sample Execution: 
spark-submit --deploy-mode client --class net.sansa_stack.kgml.rdf.ModuleExecutor --master spark://172.18.160.16:6066 --total-executor-cores 42 --executor-memory 60G --driver-memory 128G --conf "spark.executor.heartbeatInterval=100s" --conf "spark.network.timeout=2500s" SANSA-KGML/target/SANSA-KGML-0.2.0.jar WordNetEvaluation1 SANSA-KGML/datasets/yagoMovies.nt SANSA-KGML/datasets/dbpediaMovies.nt tab tab hide

#### The parameters: 
First parameter chooses a module to run it can be one of these:

PredicateStats,CommonPredicateStats,RankByPredicateType,PredicateMatching,PredicatePartitioning,
BlockSubjectsByTypeAndLiteral,CountSameASLinks,EntityMatching,deductRelations, WordNetEvaluation1, WordNetEvaluation2


Second and third parameter are dataset paths.

4th and 5th parameters are delimiteres of the first and second dataset: It can be "tab" or "space"


6th paramer is to choose to show or hide reports while running the modules. It can be "hide" or "show"
