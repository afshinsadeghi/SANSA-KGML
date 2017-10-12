package net.sansa_stack.kgml.rdf

import net.didion.jwnl.data.POS

package object wordnet {
  type Synset = net.didion.jwnl.data.Synset
  type POS    = net.didion.jwnl.data.POS

  val Noun      = POS.NOUN
  val Verb      = POS.VERB
  val Adjective = POS.ADJECTIVE
  val Adjverb   = POS.ADVERB
}
