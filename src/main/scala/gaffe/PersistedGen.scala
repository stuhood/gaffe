
package gaffe

import gaffe.AvroUtils._
import gaffe.io._
import gaffe.PersistedGen._

import java.io.File

import org.apache.avro.generic.GenericArray

/**
 * A generation that has been persisted to disk as one or more 'Views' of the same base data.
 */
object PersistedGen {
    // handle for a PersistedGen
    case class Descriptor(generation: Long, directory: File)

    /**
     * Writes Views of a MemoryGen to disk as a PersistedGen.
     */
    class Writer(desc: Descriptor, metas: List[ViewMetadata]) {
        def write(gen: MemoryGen): PersistedGen = {
            // write each view
            val views = metas.zipWithIndex.map(v => writeView(gen, v._1, v._2))
            new PersistedGen(desc, views.toList)
        }
        
        private def writeView(gen: MemoryGen, meta: ViewMetadata, idx: Int): View = {
            val viewdesc = View.Descriptor(idx, desc)
            val writer = new View.Writer(viewdesc, meta)
            try writer.write(gen) finally writer.close
            new View(viewdesc, meta)
        }
    }
}

class PersistedGen(descriptor: Descriptor, val views: List[View]) {

    /**
     * Gets views matching the given parameters.
     */
    def viewsFor(depth: Int = 1, inverted: Boolean = false) =
        views.filter(v => v.meta.depth == depth && v.meta.inverted == inverted)

    override def toString: String = {
        "#<PersistedGen %s %s>".format(descriptor, views)
    }
}

