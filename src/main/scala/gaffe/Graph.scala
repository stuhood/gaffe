
package gaffe

import java.util.TreeMap

import scala.collection.JavaConversions.asIterator

class Graph(generation: Long) {
    var version: Long = 0
    private val vertices: TreeMap[Value,Adjacencies] = new TreeMap

    /**
     * Assumes that the given odd numbered list of Values represents alternating Vertices and Edges, and adds is as a path in the graph.
     */
    def add(values: Value*): Unit = {
        if (values.size % 2 == 0)
            throw new IllegalArgumentException("value list must represent alternating vertices and edges")

        // add the source
        val iter = values.iterator
        var source = canonicalize(iter.next, vertices)
        // add the remaining path
        for (pair <- iter.sliding(2)) {
            val edge = pair(0)
            val dest = canonicalize(pair(1), vertices)

            // outbound
            val out = new Edge
            out.label = edge
            out.vertex = dest.vertex
            source.outs.put(out, 0)
            // inbound
            val in = new Edge
            in.label = edge
            in.vertex = source.vertex
            dest.ins.put(in, 0)

            source = dest
        }
        version = 1 + version
    }

    /**
     * Returns an iterator over paths matching the given odd numbered list of Values (representing alternating
     * Vertices and Edges), where option None indicates 'any value'.
     */
    def get(values: Option[Value]*): Iterator[Seq[Value]] = {
        throw new RuntimeException("Not implemented")
    }

    /**
     * Adds a value to the given vertices, and returns the canonical version of the vertex and its adjacencies.
     */
    private def canonicalize(value: Value, vertices: TreeMap[Value,Adjacencies]): Adjacencies = {
        vertices.get(value) match {
            case null =>
                // place it in the graph
                val adjacencies = new Adjacencies(new Vertex, new TreeMap, new TreeMap)
                adjacencies.vertex.name = value
                adjacencies.vertex.gen = -1
                adjacencies.vertex.block = -1
                vertices.put(value, adjacencies)
                adjacencies
            case x =>
                x
        }
    }

    override def toString: String = {
        "#<Graph %d %s>".format(version, vertices.keySet)
    }

    // adds adjacency lists to a Vertex
    final class Adjacencies(val vertex: Vertex, val ins: TreeMap[Edge, Long], val outs: TreeMap[Edge, Long])
}

