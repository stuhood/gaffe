
package gaffe

import gaffe.AvroUtils._
import gaffe.io._
import gaffe.PersistedGen._

import scala.collection.JavaConversions.asIterable

import java.io.File

import org.apache.avro.generic.GenericArray

/**
 * A generation that has been persisted to disk as one or more 'Views' of the same base data.
 */
object PersistedGen {
    // handle for a PersistedGen
    case class Descriptor(generation: Long, directory: File)

    private def createEdge(edge: MemoryGen.Edge): Edge = {
        val aedge = new Edge
        reuseEdge(aedge, edge)
        aedge
    }

    private def reuseEdge(aedge: Edge, edge: MemoryGen.Edge): Unit = {
        aedge.label = edge.label
        aedge.vertex = new Vertex
        aedge.vertex.name = edge.vertex.name
    }

    /**
     * Writes Views of a MemoryGen to disk as a PersistedGen.
     *
     * TODO: all edges for a vertex are currently stored in one chunk: implement
     * splitting edges into multiple chunks
     */
    class Writer(desc: Descriptor, metas: List[ViewMetadata]) {
        def write(generation: MemoryGen): PersistedGen = {
            // reusable persistence objects
            val range = new Range; range.begin = new Path; range.end = new Path
            range.begin.vertex = new Vertex
            range.begin.edges = genarray(Edge.SCHEMA$, 1): GenericArray[Edge]
            range.begin.edges.add(EEDGE)
            range.end.vertex = new Vertex
            range.end.edges = genarray(Edge.SCHEMA$, 1): GenericArray[Edge]
            range.end.edges.add(NEDGE)
            val edges = genarray(Edge.SCHEMA$, 1024): GenericArray[Edge]

            // write each view
            val views = {for ((meta, idx) <- metas.zipWithIndex) yield {
                val viewdesc = View.Descriptor(idx, desc)
                val writer = new View.Writer(viewdesc, meta)
                try for (vertex <- generation.iterator) {
                    // update range
                    range.begin.vertex.name = vertex.name
                    range.end.vertex.name = vertex.name
                    // copy appropriate edge values for this view
                    val adjedges = if (meta.inverted) vertex.ins else vertex.outs
                    val flatedges = for (outer <- adjedges.values; inner <- outer.values) yield inner
                    reuseArray[MemoryGen.Edge,Edge](flatedges, edges, createEdge, reuseEdge)
                    // and append
                    writer.append(range, edges)
                } finally writer.close
                new View(viewdesc)
            }}.toList

            new PersistedGen(desc, views)
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

