
package gaffe

import gaffe.AvroUtils._
import gaffe.MemoryGen._
import gaffe.Query.{Point, Segment, Range}

import java.util.{Comparator, SortedSet, TreeSet}

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

object MemoryGen {
    final case class Edge(val src: Value, val label: Value, val dest: Value)
    
    object OutComparator extends Comparator[Edge] {
        override def compare(o1: Edge, o2: Edge): Int = {
            var c = o1.src.compareTo(o2.src)
            if (c != 0) return c
            c = o1.label.compareTo(o2.label)
            if (c != 0) return c
            return o1.dest.compareTo(o2.dest)
        }
    }

    object InComparator extends Comparator[Edge] {
        override def compare(o1: Edge, o2: Edge): Int = {
            var c = o1.dest.compareTo(o2.dest)
            if (c != 0) return c
            c = o1.label.compareTo(o2.label)
            if (c != 0) return c
            return o1.src.compareTo(o2.src)
        }
    }
}

class MemoryGen(val generation: Long) {
    val vertices = new TreeSet[Value]
    val outbound = new TreeSet[Edge](OutComparator)
    val inbound = new TreeSet[Edge](InComparator)

    /**
     * Assumes that the given odd numbered list of Values represents alternating Vertices and Edges, and adds it as a path in the graph.
     */
    def add(path: List[Value]): Unit = {
        if (path.size % 2 == 0)
            throw new IllegalArgumentException("value list must represent alternating vertices and edges")

        // add the source
        val iter = path.iterator
        var src = iter.next
        maybeCreate(src)
        // and the rest of the path
        for (edge :: dest :: Nil <- iter.sliding(2)) {
            maybeCreate(dest)
            maybeCreate(src, edge, dest)
            src = dest
        }
    }

    /**
     * Returns a Stream of paths matching the given odd numbered list of Values (representing
     * alternating Vertices and Edges).
     */
    def get(query: Query): Stream[List[Value]] = query.path match {
        case Nil =>
            Stream.empty
        case srcq :: Nil =>
            // query with no edges
            srcq.segments.flatMap(_ match {
                    case Point(vertex) =>
                        if (vertices.contains(vertex))
                            Stream(List(vertex))
                        else
                            Stream.empty
                    case Range(begin, end) =>
                        vertices.subSet(begin, end).map(List(_))
                }).take(query.limit)
        case srcq :: edgeq :: destq :: xs =>
            // query a level of edges and recurse on the discovered dest vertices
            get(srcq.segments, edgeq, destq).flatMap(e =>
                get(e.dest, xs, e.label :: e.src :: Nil)).take(query.limit)
        case _ =>
            throw new IllegalArgumentException("queries alternate vertices and edges")
    }

    private def get(src: Value, query: List[Query.Clause], stack: List[Value]): Stream[List[Value]] = query match {
        case Nil =>
            // tail of query
            Stream((src :: stack).reverse)
        case edgeq :: destq :: xs =>
            // query a level of edges and recurse
            get(Stream(Point(src)), edgeq, destq).flatMap(e =>
                get(e.dest, xs, e.label :: e.src :: stack))
        case _ =>
            throw new IllegalArgumentException("queries alternate vertices and edges")
    }

    private def get(src: Stream[Segment], labelq: Query.Clause, destq: Query.Clause): Stream[Edge] =
        get(src, labelq.segments, labelq.segments, destq.segments, destq.segments)

    private def get(src: Stream[Segment], label: Stream[Segment], labelf: ()=>Stream[Segment], dest: Stream[Segment], destf: ()=>Stream[Segment]): Stream[Edge] = {
        if (src isEmpty)
            return Stream.empty
        if (label isEmpty)
            // move to the next value at the src level, and reset the two lower levels
            return get(src.tail, labelf(), labelf, destf(), destf)
        if (dest isEmpty)
            // reset the dest level, and move to the next value in the label level
            return get(src, label.tail, labelf, destf(), destf)
        // deal with a single segment, and recurse
        get(src.head, label.head, dest.head).append(get(src, label, labelf, dest.tail, destf))
    }

    /**
     * Composes value/range segments and queries outbound edges.
     * TODO: Should determine whether src or dest is more specific.
     */
    private def get(src: Segment, label: Segment, dest: Segment): Stream[Edge] = {
        (src, label, dest) match {
            case (Point(sv), Point(lv), Point(dv)) =>
                // all points
                val edge = Edge(sv, lv, dv)
                if (outbound contains edge) Stream(edge) else Stream.empty
            case (Point(sv), Point(lv), Range(db,de)) =>
                // range under points
                val begin = Edge(sv, lv, db)
                val end = Edge(sv, lv, de)
                outbound.subSet(begin, end).iterator.toStream
            case (Point(sv), Range(lb,le), _) =>
                // range under point: recurse on each unique label
                val begin = Edge(sv, lb, EVALUE)
                val end = Edge(sv, le, NVALUE)
                uniqueLabels(outbound.subSet(begin, end)).flatMap(point => get(src, point, dest))
            case (Range(sb,se), _, _) =>
                // src range: recurse on each matching Point
                vertices.subSet(sb, se).toStream.flatMap(point => get(Point(point), label, dest))
        }
    }

    /**
     * Collects unique edge labels from a sorted sequence.
     * TODO: conditionally filter Edges, rather than recursing on unique labels.
     */
    private def uniqueLabels(edges: SortedSet[Edge]): Stream[Point] = {
        val iter = edges.iterator
        if (!iter.hasNext)
            return Stream.empty
        // collect unique values
        val builder = Buffer.newBuilder[Point]
        var previous = iter.next.label
        builder += Point(previous)
        for (current <- iter if !previous.equals(current.label)) {
            builder += Point(current.label)
            previous = current.label
        }
        builder.result.toStream
    }

    /**
     * @return An iterator over graph vertices in sorted order.
     */
    def outboundIterator(): Iterator[Edge] = outbound.iterator
    def inboundIterator(): Iterator[Edge] = inbound.iterator

    /**
     * If it doesn't already exist, adds a copy of the edge to the set of edges.
     */
    private def maybeCreate(src: Value, label: Value, dest: Value): Unit = {
        val edge = Edge(src, label, dest)
        if (outbound.contains(edge)) return;
        outbound.add(edge)
        inbound.add(edge)
    }

    /**
     * If it doesn't already exist, adds a copy of the vertex to the set of vertices.
     */
    private def maybeCreate(value: Value): Unit = if (!vertices.contains(value)) {
        vertices.add(value)
    }

    override def toString: String = {
        "#<MemoryGen %s>".format(outbound)
    }
}

