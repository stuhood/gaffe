
package gaffe

import gaffe.Markset._

import java.nio.ByteBuffer
import org.apache.lucene.util.OpenBitSet

/**
 * Wraps an OpenBitSet to represent bit flags for each tuple in a Chunk.
 */
object Markset {
    val BITS_PER = 3

    /** Creates a markset with the room for the given number of tuples. */
    def apply(capacity: Int): Markset =
        new Markset(new OpenBitSet(BITS_PER * capacity))
}

class Markset(bits: OpenBitSet) {
    // count of bits used to represent tuples
    var bitpos = 0
    var parent = false

    /**
     * Toggle the parent flag to indicate that we are now positioned for a new parent.
     */
    def toggle() = { parent = !parent }

    /**
     * Sets the given flags for our current position, and increments the position.
     */
    def append(deleted: Boolean, definite: Boolean) = {
        bitpos += BITS_PER
        bits.ensureCapacity(bitpos)
        if (parent) bits.fastSet(bitpos - 3) else bits.fastClear(bitpos - 3)
        if (deleted) bits.fastSet(bitpos - 2) else bits.fastClear(bitpos - 2)
        if (definite) bits.fastSet(bitpos - 1) else bits.fastClear(bitpos - 1)
    }

    /**
     * Resets this Markset, preserving it for reuse.
     */
    def clear() = { bitpos = 0 }

    /**
     * Serializes the underlying OpenBitSet to a ByteBuffer.
     * NB: To avoid boxing, we use raw bytes rather than Avro's array of longs.
     */
    def serialize(buffer: ByteBuffer) = {
        // the number of words we've stored
        val words = OpenBitSet.bits2words(bitpos)
        val bitarr = bits.getBits
        // serialize
        buffer.asLongBuffer.put(bits.getBits, 0, words)
    }

    /**
     * Deserializes 'count' tuples into this Markset, expanding it if necessary.
     */
    def deserialize(buffer: ByteBuffer, count: Int) = {
        clear()
        // the number of words that need deserializing
        val words = OpenBitSet.bits2words(BITS_PER * count)
        bits.ensureCapacityWords(words)
        // deserialize
        buffer.asLongBuffer.get(bits.getBits, 0, words)
    }

    override def toString: String = {
        "#<Markset %d %s>".format(bitpos, bits)
    }
}

