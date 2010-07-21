
package gaffe

import gaffe.MemoryGen._

import java.util.TreeMap

import scala.collection.JavaConversions.asIterator

object MemoryGen {
    // a Vertex with adjacency lists
    final case class Vertex(val name: Value, val ins: AdjacencyList, val outs: AdjacencyList) {
        def this(name: Value) = this(name, new AdjacencyList, new AdjacencyList)

        def addOut(label: Value, vertex: Vertex) =
            add(outs, label, vertex.name, Edge(label, vertex))

        def addIn(label: Value, vertex: Vertex) =
            add(ins, vertex.name, label, Edge(label, vertex))
        
        private def add(edges: AdjacencyList, first: Value, second: Value, edge: Edge) = {
            val innermap = edges.get(first) match {
                case null =>
                    val x = new TreeMap[Value, Edge]
                    edges.put(first, x)
                    x
                case x => x
            }
            innermap.put(second, edge)
        }
    }

    // simple inbound/outbound edge class
    final case class Edge(val label: Value, val vertex: Vertex)

    // maps 'edgeval -> destval -> edge', or 'destval -> edgeval -> edge'
    type AdjacencyList = TreeMap[Value, TreeMap[Value, Edge]]
}

class MemoryGen(val generation: Long) {
    var version: Long = 0
    private val vertices: TreeMap[Value,Vertex] = new TreeMap

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

            // add edges
            source.addOut(edge, dest)
            dest.addIn(edge, source)

            source = dest
        }
        version = 1 + version
    }

    /**
     * Returns a Stream of paths matching the given odd numbered list of Values (representing
     * alternating Vertices and Edges).
     */
    def get(query: Query): Stream[List[Value]] = query.path match {
        case srcq :: xs =>
            // recurse for each matching intial vertex, and flatten the results
            srcq.filter(vertices).flatMap(vertex => getEdges(vertex.outs, xs, vertex.name :: Nil)).take(query.limit)
        case Nil =>
            throw new IllegalArgumentException("query must contain at least one value")
    }

    /**
     * Having matched an edge type, query for (edge,dest) pairs in 'within'.
     */
    private def getDests(within: TreeMap[Value,Edge], query: List[Query.Clause], stack: List[Value]): Stream[List[Value]] = query match {
        case destq :: xs =>
            destq.filter(within).flatMap(edge => getEdges(edge.vertex.outs, xs, edge.vertex.name :: edge.label :: stack))
        case Nil =>
            throw new IllegalArgumentException("value list must represent alternating vertices and edges")
    }

    /**
     * Positioned to query for edges in 'within'.
     */
    private def getEdges(within: AdjacencyList, query: List[Query.Clause], stack: List[Value]): Stream[List[Value]] = query match {
        case edgeq :: xs =>
            // recurse for matching edges
            edgeq.filter(within).flatMap(edges => getDests(edges, xs, stack))
        case Nil =>
            // previous vertex was the last in a path: return it
            Stream(stack.reverse)
    }

    /**
     * Returns an iterator over graph vertices in sorted order.
     */
    def iterator(): Iterator[Vertex] = vertices.values().iterator

    /**
     * Adds a value to the given vertices, and returns the canonical version of the vertex and its adjacencies.
     */
    private def canonicalize(value: Value): Vertex = vertices.get(value) match {
        case null =>
            // place it in the graph
            val vertex = new Vertex(value)
            vertices.put(value, vertex)
            vertex
        case x => x
    }

    override def toString: String = {
        "#<MemoryGen %d %s>".format(version, vertices.keySet)
    }
}

