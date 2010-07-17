
package gaffe

import gaffe.AvroUtils.{value, values}
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
                Identity()
            case (left, right) =>
                Range(value(left), value(right))
            case list: List[_] =>
                ValueList(values(list: _*))
            case exact =>
                Exact(value(exact))
        }
        new Query(clauses.toList)
    }

    /** Create a query for known values from a sequence of Avro Values. */
    def apply(values: List[Value]): Query = new Query(values.map(Exact(_)).toList)

    abstract class Clause() {
        def filter[T](values: NavigableMap[Value, T]): Stream[T]
    }

    /** Matches all values */
    case class Identity() extends Clause {
        override def filter[T](values: NavigableMap[Value, T]): Stream[T] =
            values.values.iterator.toStream
    }

    /** Matches a single known value */
    case class Exact(value: Value) extends Clause {
        override def filter[T](values: NavigableMap[Value, T]): Stream[T] = {
            values.get(value) match {
                case null => Stream.empty
                case x => Stream(x)
            }
        }
    }

    /** Matches any of a list of values */
    case class ValueList(valuelist: List[Value]) extends Clause {
        for (left :: right :: Nil <- valuelist.sliding(2, 1))
            assert(left.compareTo(right) < 0, "unsorted value list: %s !< %s".format(left, right))

        // TODO: should use alternative implementations based on relative size
        override def filter[T](values: NavigableMap[Value, T]): Stream[T] =
            filter(valuelist, values)

        def filter[T](remainder: List[Value], values: NavigableMap[Value, T]): Stream[T] = {
            remainder match {
                case x :: xs =>
                    values.get(x) match {
                        case null => filter(xs, values)
                        case matched => Stream.cons(matched, filter(xs, values))
                    }
                case Nil => Stream.empty
            }
        }
    }

    /** Matches any values falling between the given begin (inclusive) and end (exclusive) */
    case class Range(begin: Value, end: Value) extends Clause {
        override def filter[T](values: NavigableMap[Value, T]): Stream[T] =
            values.subMap(begin, end).values.iterator.toStream
    }
}

class Query(val path: List[Clause], val limit: Int = 1000)

