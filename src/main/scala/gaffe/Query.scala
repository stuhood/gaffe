
package gaffe

import gaffe.AvroUtils.{EVALUE, NVALUE, value, values}
import gaffe.Query._

import java.util.NavigableMap

import scala.collection.JavaConversions.asIterator

/**
 * A structural query, which is itself a graph represented by a path containing known
 * and unknown Values.
 */
object Query {
    /** Convenience constructor that stringifies each clauseval into a Clause based on type. */
    def apply(clausevals: Any*): Query = {
        val clauses = for (clause <- clausevals) yield clause match {
            case null =>
                IdentityClause()
            case (left, right) =>
                RangeClause(value(left), value(right))
            case list: List[_] =>
                ListClause(values(list: _*))
            case exact =>
                ExactClause(value(exact))
        }
        new Query(clauses.toList)
    }

    /** Create a query for known values from a sequence of Avro Values. */
    def apply(values: List[Value]): Query = new Query(values.map(ExactClause(_)).toList)

    // a possible matching value or possible (inclusive,exclusive) range
    abstract class Segment {
        def left(): Value
    }
    case class Point(value: Value) extends Segment {
        override def left() = value
    }
    case class Range(begin: Value, end: Value) extends Segment {
        override def left() = begin
    }

    abstract class Clause() {
        def segments(): Stream[Segment]
        // should eventually take samples of the input to provide a more exact answer
        def specificity: Int
    }

    /** Matches all values */
    val unboundedStream = Stream(Range(EVALUE, NVALUE))
    case class IdentityClause() extends Clause {
        override def segments(): Stream[Segment] = unboundedStream
        override def specificity: Int = 0
    }

    /** Matches a single known value */
    case class ExactClause(value: Value) extends Clause {
        val stream = Stream(Point(value))
        override def segments(): Stream[Segment] = stream
        override def specificity: Int = 3
    }

    /** Matches any of a list of values */
    case class ListClause(valuelist: List[Value]) extends Clause {
        for (left :: right :: Nil <- valuelist.sliding(2, 1))
            assert(left.compareTo(right) < 0, "unsorted value list: %s !< %s".format(left, right))
        
        val stream = valuelist.map(Point(_)).toStream
        override def segments(): Stream[Segment] = stream
        override def specificity: Int = 2
    }

    /** Matches any values falling between the given begin (inclusive) and end (exclusive) */
    case class RangeClause(begin: Value, end: Value) extends Clause {
        val stream = Stream(Range(begin, end))
        override def segments(): Stream[Segment] = stream
        override def specificity: Int = 1
    }
}

class Query(val path: List[Clause], val limit: Int = 1000)

