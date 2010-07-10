
package gaffe

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory}
import org.apache.avro.generic.GenericData
import org.apache.avro.specific.{SpecificRecord, SpecificDatumWriter, SpecificDatumReader}

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

object AvroUtils
{
    // unbuffered decoder factory
    private val DIRECT_DECODERS = new DecoderFactory().configureDirectDecoder(true)

    /**
     * Create a Value list from a seq of objects via toString.
     */
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

    def serialize[T <: SpecificRecord](o: T): Array[Byte] = {
        val buff = new ByteArrayOutputStream
        val enc = new BinaryEncoder(buff)
        new SpecificDatumWriter(o.getSchema()).write(o, enc)
        enc.flush()
        return buff.toByteArray
    }

    def deserialize[T >: Null <: SpecificRecord](schema: Schema, bytes: Array[Byte]): T = {
        val dec = DIRECT_DECODERS.createBinaryDecoder(bytes, null)
        return new SpecificDatumReader(schema).read(null, dec)
    }
}

