
package gaffe

import gaffe.io.{Chunk, Range}
import gaffe.PersistedGen._

import java.io.{File, FileInputStream}

import scala.collection.JavaConversions.asIterator

import org.apache.avro.file.{DataFileWriter, DataFileStream}
import org.apache.avro.generic.GenericArray
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader}

/**
 * A generation that has been persisted to disk.
 */
object PersistedGen {
    // for persisting the generation in file metadata
    val GENERATION_KEY = "generation"

    // handle for a PersistedGen
    type Descriptor = String
}

class PersistedGen(generation: Long, descriptor: Descriptor) {
    /**
     * Get the first Chunk less than or equal to the given path.
     * TODO: mostly useless
     */
    def chunk(path: Path): Option[(Range,GenericArray[Edge])]= {
        val rchunk = new Chunk
        val echunk = new Chunk
        val stream = new DataFileStream(new FileInputStream(descriptor), new SpecificDatumReader[Chunk])
        try while (stream.hasNext) {
            // chunks always come in pairs of 'range' and 'edge'
            stream.next(rchunk)
            stream.next(echunk)
            // if this chunk is <= our path, accept it
            if (path.compareTo(rchunk.value.asInstanceOf[Range].begin) >= 0)
                return Some((rchunk.value.asInstanceOf[Range], echunk.value.asInstanceOf[GenericArray[Edge]]))
        } finally {
            stream.close
        }
        None
    }

    override def toString: String = {
        "#<PersistedGen %s %s>".format(generation, descriptor)
    }
}

/**
 * Ranges and their edges should be appended to a PersistedGen in sorted order, and all Ranges must
 * be non-interecting.
 */
class PersistedGenWriter(generation: Long, descriptor: Descriptor) {
    // chunks to reuse for every append
    val rangeChunk = new Chunk
    val edgesChunk = new Chunk

    val writer = {
        val w = new DataFileWriter(new SpecificDatumWriter[Chunk])
        w.setMeta(PersistedGen.GENERATION_KEY, generation)
        w.create(Chunk.SCHEMA$, new File(descriptor))
    }

    def append(range: Range, edges: GenericArray[Edge]) = {
        assert(rangeChunk.value == null || rangeChunk.value.asInstanceOf[Range].compareTo(range) < 0,
            "chunks must be appended in ascending order")

        writer.append({rangeChunk.value = range; rangeChunk})
        writer.append({edgesChunk.value = edges; edgesChunk})
    }

    def close() = writer.close
}
