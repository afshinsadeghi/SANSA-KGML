package net.sansa_stack.kgml.rdf

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer
import org.apache.spark.serializer.KryoRegistrator

import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
/*
* Created by Shimaa
*
* */


class UnmodifiableCollectionKryoRegistrator extends KryoRegistrator  {

  override def registerClasses(kryo: Kryo): Unit = {
    val cls = Class.forName("edu.cmu.lti.ws4j.impl.JiangConrath")
    kryo.addDefaultSerializer(cls, new UnmodifiableCollectionsSerializer)
  }
}

class Registrator extends SparkKryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    // model
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.JiangConrath])
    kryo.register(classOf[net.sansa_stack.kgml.rdf.wordnet.WordNet])
    kryo.register(classOf[net.sansa_stack.kgml.rdf.PredicatesSimilarity])
    kryo.register(classOf[net.sansa_stack.kgml.rdf.SimilarityHandler])

  }
}