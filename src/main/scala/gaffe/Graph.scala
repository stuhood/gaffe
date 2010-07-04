
package gaffe

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.JavaConversions.asIterator

class Graph(generation: Long) {
    // a graph is an immutable set of edges and vertices
    private val ref: AtomicReference[GraphRef] = new AtomicReference(new GraphRef(0, HashMap.empty, HashSet.empty))

    /**
     * Atomically adds the given paths to this graph.
     */
    def add(paths: Path*): Unit = {
        while (true) {
            // atomically get a graph reference
            val graph = ref.get

            // add each path to the graph
            var vertices: (Vertex, HashMap[Value, Vertex]) = (null, graph.vertices)
            var edges = graph.edges
            for (path <- paths) {
                vertices = canonicalize(path.source, vertices._2)
                for (edge <- path.edges.iterator) {
                    val source = vertices._1
                    vertices = canonicalize(edge.dest, vertices._2)
                    // clone the outbound edge with the canonicalized destination
                    val e = new Edge
                    e.label = edge.label
                    e.weight = 0 // FIXME
                    e.dest = vertices._1
                    // and add to the set of edges
                    edges = edges + new OutEdge(source, e)
                }
            }

            // update the graph reference
            if (ref.compareAndSet(graph, new GraphRef(graph.version + 1, vertices._2, edges)))
                return;
        }
    }

    /**
     * The current version of the graph.
     */
    def version = ref.get.version

    /**
     * Adds a vertex to the given vertices, and returns the canonical version of the vertex and the new vertices.
     */
    private def canonicalize(vertex: Vertex, vertices: HashMap[Value,Vertex]): (Vertex, HashMap[Value,Vertex]) = vertices.get(vertex.name) match {
        case null =>
            // clone the vertex and place it in the graph
            val v = new Vertex
            v.name = vertex.name
            v.gen = vertex.gen
            v.block = vertex.block
            (v, vertices.updated(v.name, v))
        case _ =>
            (vertex, vertices)
    }

    override def toString: String = ref.get.toString

    // adds a source Vertex to Edge
    final class OutEdge(vertex: Vertex, edge: Edge) extends Tuple2(vertex, edge)

    // an immutable reference to a graph (intended for quick comparison)
    final class GraphRef(val version: Long, val vertices: HashMap[Value,Vertex], val edges: HashSet[OutEdge])
    {
        override def hashCode = this.version.hashCode

        override def equals(o: Any): Boolean = o match {
            case that: GraphRef => this.version == that.version
            case _ => false
        }

        override def toString: String = {
            "#<Graph %d %s %s>".format(version, vertices.keySet, edges)
        }
    }
}

