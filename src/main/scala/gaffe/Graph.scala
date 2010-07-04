
package gaffe

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.{HashMap, HashSet}
import scala.collection.JavaConversions.asIterator

class Graph(generation: Long) {
    var version: Long = 0
    private val vertices: HashMap[Value,Vertex] = HashMap.empty
    private val edges: HashSet[OutEdge] = HashSet.empty

    /**
     * Atomically adds the given paths to this graph.
     */
    def add(paths: Path*): Unit = {
        // add each path to the graph
        for (path <- paths) {
            var source = canonicalize(path.source, vertices)
            for (edge <- path.edges.iterator) {
                val dest = canonicalize(edge.dest, vertices)
                // clone the outbound edge with the canonicalized destination
                val e = new Edge
                e.label = edge.label
                e.weight = 0 // FIXME
                e.dest = dest
                // and add to the set of edges
                edges + new OutEdge(source, e)
                source = dest
            }
        }
        version = version + 1
    }

    /**
     * Adds a vertex to the given vertices, and returns the canonical version of the vertex and the new vertices.
     */
    private def canonicalize(vertex: Vertex, vertices: HashMap[Value,Vertex]): Vertex = vertices.get(vertex.name) match {
        case null =>
            // clone the vertex and place it in the graph
            val v = new Vertex
            v.name = vertex.name
            v.gen = vertex.gen
            v.block = vertex.block
            vertices.put(v.name, v)
            v
        case _ =>
            vertex
    }

    override def toString: String = {
        "#<Graph %d %s %s>".format(version, vertices.keySet, edges)
    }

    // adds a source Vertex to Edge
    final class OutEdge(vertex: Vertex, edge: Edge) extends Tuple2(vertex, edge)
}

