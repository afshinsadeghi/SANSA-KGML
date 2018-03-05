package net.sansa_stack.kgml.rdf

/**
  * Created by afshin on 12.10.17.
  */
import net.didion.jwnl.data.POS
import net.sansa_stack.kgml.rdf.wordnet.WordNet

object TestWordNetSimilarityMeasures {
  def main(args: Array[String]) = {
    Console.println(">>> dog = wn.synset('dog.n.01')")
    Console.println(">>> cat = wn.synset('cat.n.01')")
    Console.println(">>> hit = wn.synset('hit.v.01')")
    Console.println(">>> slap = wn.synset('slap.v.01')")
    Console.println(">>> car = wn.synset('car.n.01')")
    Console.println(">>> bus = wn.synset('bus.n.01')")

    val wn =  new WordNet

    val dog = wn.synset("dog", POS.NOUN, 1)
    val cat = wn.synset("cat", POS.NOUN, 1)
    val hit = wn.synset("hit", POS.VERB, 1)
    val slap = wn.synset("slap", POS.VERB, 1)
    val car = wn.synset("car", POS.NOUN, 1)
    val bus = wn.synset("bus", POS.NOUN, 1)
    //    val car   = wn.synset("weak"    , POS.ADJECTIVE, 1)
    //    val bus   = wn.synset("physical", POS.ADJECTIVE, 1)

    Console.println(">>> dog.path_similarity(cat)")
    val dogCatPathSimilarity = wn.pathSimilarity(dog, cat)
    Console.println(dogCatPathSimilarity)
    Console.println(">>> hit.path_similarity(slap)")
    val hitSlapPathSimilarity = wn.pathSimilarity(hit, slap)
    Console.println(hitSlapPathSimilarity)

    Console.println(">>> dog.lch_similarity(cat)")

    val dogCatLchSimilarity = wn.lchSimilarity(dog, cat)
    Console.println(dogCatLchSimilarity)
    Console.println(">>> hit.lch_similarity(slap)")

    val hitSlapLchSimilarity = wn.lchSimilarity(hit, slap)
    Console.println(hitSlapLchSimilarity)

    Console.println(">>> dog.wup_similarity(cat)")
    val dogCatWupSimilarity = wn.wupSimilarity(dog, cat)
    Console.println(dogCatWupSimilarity)
    Console.println(">>> hit.wup_similarity(slap)")
    val hitSlapWupSimilarity = wn.wupSimilarity(hit, slap)
    Console.println(hitSlapWupSimilarity)

    Console.println(">>> dog.res_similarity(cat)")
    val dogCatResSimilarity = wn.resSimilarity(dog, cat)
    Console.println(dogCatResSimilarity)
    Console.println(">>> hit.res_similarity(slap)")

//    Console.println(">>> dog.jcn_similarity(cat)")
//    val dogCatJcnSimilarity = wn.jcnSimilarity(dog, cat)
//    Console.println(dogCatJcnSimilarity)

    Console.println(">>> dog.lin_similarity(cat)")
    val dogCatLinSimilarity = wn.linSimilarity(dog, cat)
    Console.println(dogCatLinSimilarity)

    Console.println(">>> car.lesk_similarity(bus)")
    val carBusLeskSimilarity = wn.leskSimilarity(car, bus)
    Console.println(carBusLeskSimilarity)
  }
}