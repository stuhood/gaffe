
package gaffe

import gaffe.MemoryGen._

import java.util.TreeMap

import scala.collection.JavaConversions.asIterator

object MemoryGen {
    // simple inbound/outbound edge class
    final case class Edge(val label: Value, val vertex: Vertex)

    // maps 'edgeval -> destval -> edge', or 'destval -> edgeval -> edge'
    type AdjacencyList = TreeMap[Value, TreeMap[Value, Edge]]

    // a Vertex with adjacency lists
    final case class Vertex(val name: Value, val ins: AdjacencyList, val outs: AdjacencyList) {
        def this(name: Value) = this(name, new AdjacencyList, new AdjacencyList)

        // temporary
        def getOut(label: Value, vertex: Value): Option[Edge] = outs.get(label) match {
            case null => None
            case innermap =>
                innermap.get(vertex) match {
                    case null => None
                    case x => Some(x)
                }
        }

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
            val edge = src.getOut(edgev.value, destv.value)
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
     * Returns an iterator over graph Vertex in sorted order.
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

