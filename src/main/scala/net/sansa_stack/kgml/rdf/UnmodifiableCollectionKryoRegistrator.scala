package net.sansa_stack.kgml.rdf

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer
import org.apache.spark.serializer.KryoRegistrator


class UnmodifiableCollectionKryoRegistrator extends KryoRegistrator  {

  override def registerClasses(kryo: Kryo): Unit = {
    val cls = Class.forName("PredicatesSimilarity")
    kryo.addDefaultSerializer(cls, new UnmodifiableCollectionsSerializer)
  }
}