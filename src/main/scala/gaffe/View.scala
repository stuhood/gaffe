
package gaffe

import gaffe.AvroUtils._
import gaffe.io._
import gaffe.View._

import java.io.File

import org.apache.avro.file.{DataFileWriter, DataFileReader}
import org.apache.avro.generic.GenericArray
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader}
import org.apache.avro.Schema

/**
 * A View is a sorted list of paths contained in a persisted generation. Views have metadata
 * that describes the data they store, such as whether they store inbound or outbound edges
 * and their depth.
 * TODO: allow for 'partial' views, storing only a portion of the data for a generation
 */
object View {
    // for persisting the generation in file metadata
    val VIEW_META_SCHEMA_KEY = "view_meta_schema"
    val VIEW_META_KEY = "view_meta"

    /**
     * Construct metadata for a view.
     */
    def metadata(generation: Long, id: Long, depth: Int, inverted: Boolean, partial: Boolean) = {
        val meta = new ViewMetadata
        meta.generation = generation
        meta.id = id
        meta.depth = depth
        meta.inverted = inverted
        meta.partial = partial
        meta
    }

    case class Descriptor(id: Long, gen: PersistedGen.Descriptor) {
        /**
         * Calculate the filename for a View component.
         */
        def filename(component: String): File = {
            val name = "gen-%d-view-%d-%s.gaffe".format(gen.generation, id, component)
            new File(gen.directory, name)
        }
    }

    /**
     * Ranges and their edges are appended to a view in sorted order, and all Ranges
     * must be non-interecting.
     */
    class Writer(desc: Descriptor, meta: ViewMetadata) {
        // chunks to reuse for every append
        val rangeChunk = new Chunk
        val edgesChunk = new Chunk

        val writer = {
            val w = new DataFileWriter(new SpecificDatumWriter[Chunk])
            w.setMeta(VIEW_META_SCHEMA_KEY, meta.getSchema().toString)
            w.setMeta(VIEW_META_KEY, serialize(meta))
            w.create(Chunk.SCHEMA$, desc.filename("data"))
        }

        def append(range: Range, edges: GenericArray[Edge]) = {
            writer.append({rangeChunk.value = range; rangeChunk})
            writer.append({edgesChunk.value = edges; edgesChunk})
        }

        def close() = writer.close
    }
}

class View(desc: Descriptor) {
    // reusable state
    val dreader = new SpecificDatumReader[Chunk]

    // metadata
    val meta: ViewMetadata = {
        // deserialize metadata from the data component
        val r = new DataFileReader(desc.filename("data"), dreader)
        try {
            val schema = Schema.parse(r.getMetaString(VIEW_META_SCHEMA_KEY))
            deserialize(schema, r.getMeta(VIEW_META_KEY))
        } finally
            r.close
    }

    /**
     * Get the first Chunk less than or equal to the given path.
     * TODO: mostly useless
     */
    def chunk(path: Path): Option[(Range,GenericArray[Edge])] = {
        val rchunk = new Chunk
        val echunk = new Chunk
        val reader = new DataFileReader(desc.filename("data"), dreader)
        try while (reader.hasNext) {
            // chunks always come in pairs of 'range' and 'edge'
            reader.next(rchunk)
            reader.next(echunk)
            // if this chunk is <= our path, accept it
            if (path.compareTo(rchunk.value.asInstanceOf[Range].begin) >= 0)
                return Some((rchunk.value.asInstanceOf[Range], echunk.value.asInstanceOf[GenericArray[Edge]]))
        } finally {
            reader.close
        }
        None
    }

    override def toString: String = {
        "#<View %d %s>".format(desc.id, meta)
    }
}

