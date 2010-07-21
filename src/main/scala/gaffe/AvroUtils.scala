
package gaffe

import gaffe.io._

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

    /** Copies a ByteBuffer. */
    def copy(buff: ByteBuffer): ByteBuffer = {
        val clone = ByteBuffer.allocate(buff.remaining)
        clone.put(buff)
        buff.rewind
        clone.flip
        clone
    }

    /** Create a Value from an object via toString. */
    def value(ob: Any): Value = {
        val value = new Value
        value.value = if (ob == null) null else ByteBuffer.wrap(ob.toString.getBytes)
        value
    }

    /** Create a Value list from a seq of objects via toString. */
    def values(obs: Any*): List[Value] = obs.map(value).toList

    /**
     * Reuses the given GenericArray and object content.
     */
    def reuseArray[I,C](obs: Iterable[I], array: GenericArray[C], create: I => C, reuse: (C, I) => Unit): Unit = {
        array.clear
        val obsiter = obs.iterator
        val arrayiter = array.iterator

        // reuse as many objects as already exist in the array
        while (obsiter.hasNext && arrayiter.hasNext)
            reuse(arrayiter.next, obsiter.next)

        // and expand for the remainder
        while (obsiter.hasNext)
            array.add(create(obsiter.next))
    }

    def genarray[T](schema: Schema, size: Int): GenericArray[T] =
        new GenericData.Array[T](size, Schema.createArray(schema))

    def genarray[T](schema: Schema, values: T*): GenericArray[T] = {
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

