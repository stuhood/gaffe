
package gaffe

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.{HashMap, HashSet}
import scala.collection.JavaConversions.asIterator

class Graph(generation: Long) {
    var version: Long = 0
    private val vertices: HashMap[Value,Adjacencies] = HashMap.empty

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
            source.outs + out
            // inbound
            val in = new Edge
            in.label = edge
            in.vertex = source.vertex
            dest.ins + in

            source = dest
        }
        version = version + 1
    }

    /**
     * Adds a value to the given vertices, and returns the canonical version of the vertex and its adjacencies.
     */
    private def canonicalize(value: Value, vertices: HashMap[Value,Adjacencies]): Adjacencies = {
        vertices.get(value).getOrElse({
            // place it in the graph
            val adjacencies = new Adjacencies(new Vertex, Set.empty, Set.empty)
            adjacencies.vertex.name = value
            adjacencies.vertex.gen = -1
            adjacencies.vertex.block = -1
            vertices.put(value, adjacencies)
            adjacencies})
    }

    override def toString: String = {
        "#<Graph %d %s>".format(version, vertices.keySet)
    }

    // adds adjacency lists to a Vertex
    final class Adjacencies(val vertex: Vertex, val ins: Set[Edge], val outs: Set[Edge])
}

