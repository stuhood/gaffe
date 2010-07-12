
package gaffe

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory}
import org.apache.avro.generic.{GenericArray, GenericData}
import org.apache.avro.specific.{SpecificRecord, SpecificDatumWriter, SpecificDatumReader}

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

object AvroUtils
{
    // unbuffered decoder factory
    private val DIRECT_DECODERS = new DecoderFactory().configureDirectDecoder(true)

    // empty values sort first, null values sort last
    val EVALUE = {val v = new Value; v.value = ByteBuffer.allocate(0); v}
    val NVALUE = {val v = new Value; v.value = null; v}
    val EVERTEX = {val v = new Vertex; v.name = EVALUE; v}
    val NVERTEX = {val v = new Vertex; v.name = NVALUE; v}
    val EEDGE = {val e = new Edge; e.label = EVALUE; e.vertex = EVERTEX; e}
    val NEDGE = {val e = new Edge; e.label = NVALUE; e.vertex = NVERTEX; e}

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
        path.vertex = new Vertex; path.vertex.name = vals.head
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

    def genarray[T <: SpecificRecord](schema: Schema, size: Int): GenericArray[T] =
        new GenericData.Array[T](size, Schema.createArray(schema))

    def genarray[T <: SpecificRecord](schema: Schema, values: T*): GenericArray[T] = {
        val arr = genarray[T](schema, values.size)
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

