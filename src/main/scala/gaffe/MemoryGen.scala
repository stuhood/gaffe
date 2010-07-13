
package gaffe

import java.util.TreeMap

import scala.collection.JavaConversions.asIterator

class MemoryGen(val generation: Long) {
    var version: Long = 0
    private val vertices: TreeMap[Value,Adjacencies] = new TreeMap

    /**
     * Assumes that the given odd numbered list of Values represents alternating Vertices and Edges, and adds it as a path in the graph.
     */
    def add(path: List[Value]): Unit = {
        if (path.size % 2 == 0)
            throw new IllegalArgumentException("value list must represent alternating vertices and edges")

        // add the source
        val iter = path.iterator
        var source = canonicalize(iter.next)
        // add the remaining path
        for (pair <- iter.sliding(2)) {
            val edge = pair(0)
            val dest = canonicalize(pair(1))

            // outbound
            val out = new Edge; out.label = edge; out.vertex = dest
            source.outs.put(new SimpleEdge(out.label, out.vertex.name), out)
            // inbound
            val in = new Edge; in.label = edge; in.vertex = source
            dest.ins.put(new SimpleEdge(in.vertex.name, in.label), in)

            source = dest
        }
        version = 1 + version
    }

    /**
     * Returns a seq of paths matching the given odd numbered list of Values (representing alternating
     * Vertices and Edges).
     */
    def get(query: Query): Option[List[Value]] = get(query.path, Nil)

    /**
     * Takes a query portion, and a stack representing the currently matched path. This method
     * will recurse to find the remainder of the query portion, returning matched paths.
     */
    private def get(query: List[Query.KnownValue], stack: List[Value]): Option[List[Value]] = query match {
        case srcv :: edgev :: destv :: xs =>
            // triple of src, edge, dest
            val src = vertices.get(srcv.value)
            if (src == null) return None
            val edge = src.outs.get(new SimpleEdge(edgev.value, destv.value))
            if (edge == null) return None
            // append to the stack and recurse
            get(destv :: xs, edgev.value :: srcv.value :: stack)
        case destv :: Nil =>
            // tail of the path
            val dest = vertices.get(destv.value)
            if (dest == null) return None
            // append dest and return
            Some((destv.value :: stack).reverse)
        case _ =>
            throw new IllegalArgumentException("value list must represent alternating vertices and edges")
    }

    /**
     * Returns an iterator over graph Adjacencies in sorted order.
     */
    def iterator(): Iterator[Adjacencies] = vertices.values().iterator

    /**
     * Adds a value to the given vertices, and returns the canonical version of the vertex and its adjacencies.
     */
    private def canonicalize(value: Value): Adjacencies = vertices.get(value) match {
        case null =>
            // place it in the graph
            val adjacencies = new Adjacencies(new TreeMap, new TreeMap)
            adjacencies.name = value
            adjacencies.gen = -1
            adjacencies.block = -1
            vertices.put(value, adjacencies)
            adjacencies
        case x => x
    }

    override def toString: String = {
        "#<MemoryGen %d %s>".format(version, vertices.keySet)
    }

    // adds adjacency lists to a Vertex
    final class Adjacencies(val ins: TreeMap[SimpleEdge, Edge], val outs: TreeMap[SimpleEdge, Edge]) extends Vertex

    // the vertex may use either position, depending on whether this is an inbound or outbound edge
    final class SimpleEdge(_1: Value, _2: Value) extends Tuple2(_1, _2)
        with Comparable[SimpleEdge] {
        override def compareTo(that: SimpleEdge): Int = {
            if (this == that) return 0
            val c = _1.compareTo(that._1)
            return if (c != 0) c else _2.compareTo(that._2)
        }
    }
}

