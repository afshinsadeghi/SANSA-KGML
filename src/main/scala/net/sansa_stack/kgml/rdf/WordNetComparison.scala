package net.sansa_stack.kgml.rdf
import org.apache.spark.sql.functions._

/**
  * Created by afshin on 07.03.18.
  */
class WordNetComparison{


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


}
