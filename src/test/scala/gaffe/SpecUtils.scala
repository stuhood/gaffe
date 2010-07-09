
package gaffe

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * Test utility functions.
 */
trait SpecUtils extends Configuration
{
    def values(obs: Any*): List[Value] = {
        for (ob <- obs) yield {
            val value = new Value
            value.value = if (ob == null) null else ByteBuffer.wrap(ob.toString.getBytes)
            value
        }
    }.toList

    /**
     * Creates a Path object from a list of values.
     */
    def mkpath(obs: Any*): Path = {
        val vals = values(obs: _*)
        if (vals.size % 2 == 0) throw new IllegalArgumentException("Invalid path")

        val path = new Path
        path.source = new Vertex; path.source.name = vals.head
        path.edges = genarray[Edge](Edge.SCHEMA$,
            {for (List(label, name) <- vals.tail.sliding(2)) yield {
                val edge = new Edge
                edge.label = label
                edge.vertex = new Vertex
                edge.vertex.name = name
                edge
            }}.toSeq: _*)
        path
    }

    def genarray[T](schema: Schema, values: T*): GenericData.Array[T] = {
        val arr = new GenericData.Array[T](values.size, Schema.createArray(schema))
        for (value <- values)
            arr.add(value)
        arr
    }
}
;
