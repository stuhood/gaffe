
package gaffe

import gaffe.Query._
import gaffe.{Value => AvroV}

/**
 * A structural query, which is itself a graph represented by a path containing known
 * and unknown Values.
 *
 * TODO: currently only supports queries containing KnownValues.
 */
class Query(val path: List[KnownValue], val limit: Int = 1000)

object Query {

    /**
     * Create a query for known values from a sequence of objects via toString.
     */
    def apply(values: List[AvroV]): Query =
        new Query(values.map(KnownValue(_)).toList)

    abstract class Value()
    /** Matches a single known value */
    case class KnownValue(value: AvroV) extends Value
    /** Matches any of a list of values */
    // case class ListValue(values: List[AvroV]) extends Value
    /** Matches any values falling between the given begin (inclusive) and end (exclusive) */
    // case class RangeValue(begin: AvroV, end: AvroV) extends Value
}

