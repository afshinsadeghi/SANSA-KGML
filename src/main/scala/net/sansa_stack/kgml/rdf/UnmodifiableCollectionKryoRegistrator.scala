package net.sansa_stack.kgml.rdf

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo


import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
/*
* Created by Shimaa
*
* */
/*
 * Class for serialization by the Kryo serializer.
 */

class UnmodifiableCollectionKryoRegistrator extends KryoRegistrator  {

  override def registerClasses(kryo: Kryo): Unit = {
    val cls = Class.forName("net.sansa_stack.kgml.rdf.wordnet.WordNet")
    kryo.addDefaultSerializer(cls, new UnmodifiableCollectionsSerializer)
  }
}

class Registrator extends SparkKryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    // model
    kryo.register(classOf[net.sansa_stack.kgml.rdf.wordnet.WordNet])
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.Path])
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.JiangConrath])
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.LeacockChodorow])
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.WuPalmer])
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.Resnik])
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.Lin])
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.Lesk])
    kryo.register(classOf[net.sansa_stack.kgml.rdf.PredicatesSimilarity])
    kryo.register(classOf[net.sansa_stack.kgml.rdf.SimilarityHandler])
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.JiangConrath])
    kryo.register(classOf[edu.cmu.lti.ws4j.impl.LeacockChodorow])
  }
}

