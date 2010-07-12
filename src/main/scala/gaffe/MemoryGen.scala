
package gaffe

import java.util.TreeMap

import scala.collection.JavaConversions.asIterator

class MemoryGen(val generation: Long) {
    var version: Long = 0
    private val vertices: TreeMap[Value,Adjacencies] = new TreeMap

    /**
     * Assumes that the given odd numbered list of Values represents alternating Vertices and Edges, and adds is as a path in the graph.
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
            source.outs.put(out.label, out)
            // inbound
            val in = new Edge; in.label = edge; in.vertex = source
            dest.ins.put(in.label, in)

            source = dest
        }
        version = 1 + version
    }

    /**
     * Returns a seq of paths matching the given odd numbered list of Values (representing alternating
     * Vertices and Edges).
     * TODO: handle missing values
     */
    def get(path: List[Value]): List[Value] = path match {
        case srcv :: edgev :: destv :: xs =>
            // triple of src, edge, dest
            val src = vertices.get(srcv)
            val edge = src.outs.get(edgev)
            val dest = vertices.get(destv)
            // recurse
            src.name :: edge.label :: get(destv :: xs)
        case vertex :: Nil =>
            // tail of the path
            List(vertices.get(vertex).name)
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
    final class Adjacencies(val ins: TreeMap[Value, Edge], val outs: TreeMap[Value, Edge]) extends Vertex
}

