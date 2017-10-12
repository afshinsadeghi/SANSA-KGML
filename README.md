Maven Project for SANSA using Spark
=============================

This is a [Maven](https://maven.apache.org/)  generate a [SANSA](https://github.com/SANSA-Stack)-KGML project using [Apache Spark](http://spark.apache.org/).

Installation:
----------

```
git clone https://github.com/SANSA-Stack/SANSA-Spark-KG-ML.git
cd SANSA-Template-Maven-Spark

mvn clean package
````

### Installing wordNet database
##### In the root directory of the project run:
```
sbt download-database
```


