package net.sansa_stack.kgml.rdf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by afshin on 07.03.18.
  */
class Matching(sparkSession: SparkSession ){


  /*
  *  get last part of a URI
  */
  val getLastPartOfURI = udf((S: String) =>  {
    if(S.startsWith("<") ){
      var temp = S.split("<")(1)
      temp = temp.split(">")(0)
      temp = temp.split("\\").last
    }
    else S})

  val getLiteralValue = udf((S: String) =>  {if(S.startsWith("\"") ) S.split("\"")(1) else S})


  def MatchLiterals(df1: DataFrame, df2: DataFrame): Unit = {

    val dF1 = df1.
      withColumn("Literal1", getLiteralValue(col("object1")))

    val dF2 = df2.
      withColumn("Literal2", getLiteralValue(col("object2")))

    dF1.createOrReplaceTempView("triple")
    dF2.createOrReplaceTempView("triple2")
    val samePredicateAndObject = dF1.join(dF2, dF1("predicate1") <=> dF2("predicate2")
      && dF1("literal1") <=> dF2("literal2"))


    samePredicateAndObject.createOrReplaceTempView("sameTypes")
    println("ranking of subjects based on common type.(I used common predicate and objects which is more general than common type)")
    val sqlText2 = "SELECT  subject1, subject2, COUNT(*) FROM sameTypes group by subject1,subject2 ORDER BY COUNT(*) DESC"
    val typedTriples2 = sparkSession.sql(sqlText2)
    typedTriples2.show(15, 80)

  }
}
