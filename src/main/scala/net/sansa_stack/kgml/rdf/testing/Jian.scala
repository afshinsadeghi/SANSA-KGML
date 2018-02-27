package net.sansa_stack.kgml.rdf.testing


object JiangConrath {
//  protected var min = 0 // or -Double.MAX_VALUE ?
//
//  protected var max: Double = 1.7976931348623157e+308
//  @SuppressWarnings(Array("serial")) private val posPairs = new util.ArrayList[Array[POS]]() {}
//
//
//class JiangConrath(override val db: ILexicalDatabase) extends RelatednessCalculator(db = db) {
//  override protected def calcRelatedness(synset1: Concept, synset2: Concept): Relatedness = {
//    val tracer = new StringBuilder
//    if (synset1 == null || synset2 == null) return new Relatedness(JiangConrath.min, null, RelatednessCalculator.illegalSynset)
//    //Don't short circuit here, calculate the real value!
//    //if ( synset1.getSynset().equals( synset2.getSynset() ) ) return new Relatedness( max );
//    val subTracer: StringBuilder = if (RelatednessCalculator.enableTrace) new StringBuilder
//    else null
//
//    var lcsList = ICFinder.getInstance.getLCSbyIC(pathFinder, synset1, synset2, subTracer)
//
//      .getLCSbyIC(pathFinder, synset1, synset2, subTracer)
//    if (lcsList.size == 0) return new Relatedness(JiangConrath.min, tracer.toString, null)
//    if (RelatednessCalculator.enableTrace) {
//      tracer.append(subTracer.toString)
//      for (lcs <- lcsList) {
//        tracer.append("Lowest Common Subsumer(s): ")
//        tracer.append(db.conceptToString(lcs.subsumer.getSynset) + " (IC=" + lcs.ic + ")\n")
//      }
//    }
//    val subsumer = lcsList.get(0)
//    val lcsSynset = subsumer.subsumer.getSynset
//    val lcsIc = subsumer.ic
//    /* Commented out as maxScore is not used */
//    //int lcsFreq = ICFinder.getInstance().getFrequency( lcsSynset );
//    //double maxScore;
//    val rootSynset = pathFinder.getRoot(lcsSynset)
//    rootSynset.setPos(subsumer.subsumer.getPos)
//    val rootFreq = ICFinder.getInstance.getFrequency(rootSynset)
//    if (rootFreq > 0) {
//      //maxScore = 2D * -Math.log( 0.001D / (double)rootFreq ) + 1; // add-1 smoothing
//    }
//    else return new Relatedness(JiangConrath.min, tracer.toString, null)
//    /* Comments from WordNet::Similarity::jcn.pm:
//         * Foreach lowest common subsumer...
//         * Find the minimum jcn distance between the two subsuming concepts...
//         * Making sure that neither of the 2 concepts have 0 infocontent
//         */ val ic1 = ICFinder.getInstance.ic(pathFinder, synset1)
//    val ic2 = ICFinder.getInstance.ic(pathFinder, synset2)
//    if (enableTrace) {
//      tracer.append("Concept1: " + db.conceptToString(synset1.getSynset) + " (IC=" + ic1 + ")\n")
//      tracer.append("Concept2: " + db.conceptToString(synset2.getSynset) + " (IC=" + ic2 + ")\n")
//    }
//    var distance = 0D
//    if (ic1 > 0 && ic2 > 0) distance = ic1 + ic2 - (2 * lcsIc)
//    else return new Relatedness(JiangConrath.min, tracer.toString, null)
//    /* Comments from WordNet::Similarity jcn.pm:
//         * Now if distance turns out to be 0...
//         * implies ic1 == ic2 == ic3 (most probably all three represent
//         * the same concept)... i.e. maximum relatedness... i.e. infinity...
//         * We'll return the maximum possible value ("Our infinity").
//         * Here's how we got our infinity...
//         * distance = ic1 + ic2 - (2 x ic3)
//         * Largest possible value for (1/distance) is infinity, when distance = 0.
//         * That won't work for us... Whats the next value on the list...
//         * the smallest value of distance greater than 0...
//         * Consider the formula again... distance = ic1 + ic2 - (2 x ic3)
//         * We want the value of distance when ic1 or ic2 have information content
//         * slightly more than that of the root (ic3)... (let ic2 == ic3 == 0)
//         * Assume frequency counts of 0.01 less than the frequency count of the
//         * root for computing ic1...
//         * sim = 1/ic1
//         * sim = 1/(-log((freq(root) - 0.01)/freq(root)))
//         */ var score = 0D
//    if (distance == 0D) if (rootFreq > 0.01D) score = 1D / -Math.log((rootFreq.toDouble - 0.01D) / rootFreq.toDouble)
//    else return new Relatedness(JiangConrath.min, tracer.toString, null)
//    else { // happy path
//      score = 1D / distance
//    }
//    new Relatedness(score, tracer.toString, null)
//  }
//
//  override def getPOSPairs: util.List[Array[POS]] = JiangConrath.posPairs
//}
}
