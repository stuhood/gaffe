
package gaffe

import gaffe.AvroUtils._
import gaffe.io._
import gaffe.View._

import java.io.{Closeable, File}
import scala.collection.JavaConversions.asIterable

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

    // maximum number of paths to store in each chunk
    // (TODO: replace with size/cardinality calculation)
    val PATHS_PER_CHUNK = 1024

    /** Opens a reader for the given descriptor. */
    def apply(desc: Descriptor): View = new View(desc, loadmetadata(desc))

    /** Construct metadata for a View. */
    def metadata(generation: Long, id: Int, depth: Int, inverted: Boolean, partial: Boolean) = {
        val meta = new ViewMetadata
        meta.generation = generation
        meta.id = id
        meta.depth = depth
        meta.inverted = inverted
        meta.partial = partial
        meta
    }

    /** Load metadata for a View from disk. */
    def loadmetadata(desc: Descriptor): ViewMetadata = {
        // deserialize metadata from the data component
        val r = new DataFileReader(desc.filename("data"), new SpecificDatumReader[Chunk])
        try {
            val schema = Schema.parse(r.getMetaString(VIEW_META_SCHEMA_KEY))
            deserialize(schema, r.getMeta(VIEW_META_KEY))
        } finally
            r.close
    }

    case class Descriptor(id: Int, gen: PersistedGen.Descriptor) {
        /**
         * Calculate the filename for a View component.
         */
        def filename(component: String): File = {
            val name = "gen-%d-view-%d-%s.gaffe".format(gen.generation, id, component)
            new File(gen.directory, name)
        }
    }

    class Writer(desc: Descriptor, meta: ViewMetadata) extends Closeable {
        // reusable state
        val chunk = new Chunk
        // TODO: currently only writes chunks of depth 0 and arity 3
        chunk.depth = 0; chunk.arity = 3
        chunk.values = genarray(Value.SCHEMA$, PATHS_PER_CHUNK + chunk.arity)
        // TODO: currently only writes "high cardinality" chunks
        chunk.runlengths = genarray(Schema.create(Schema.Type.LONG), 1024)

        // avro datafile writer
        val writer = new DataFileWriter(new SpecificDatumWriter[Chunk])
        writer.setCodec(Configuration().default_codec)
        writer.setMeta(VIEW_META_SCHEMA_KEY, meta.getSchema().toString)
        writer.setMeta(VIEW_META_KEY, serialize(meta))
        writer.create(Chunk.SCHEMA$, desc.filename("data"))

        /**
         * Vertices are appended to a view in order, and each vertex results in one or
         * more paths being written in sorted order to disk.
         */
        def append(vertex: MemoryGen.Vertex) = {
            // append appropriate edge values for this view
            val adjedges = if (meta.inverted) vertex.ins else vertex.outs
            for (outer <- adjedges.values; edge <- outer.values) {
                chunk.values.add(vertex.name)
                chunk.values.add(edge.label)
                chunk.values.add(edge.vertex.name)
            }
            
            // occasionally flush
            if (chunk.values.size >= PATHS_PER_CHUNK) flush()
        }

        private def flush() = if (chunk.values.size > 0) {
            writer.append(chunk)
            chunk.values.clear
            chunk.runlengths.clear
        }

        override def close() = {
            flush()
            writer.close
        }
    }
}

/**
 * Reader for a View: not thread safe.
 */
class View(desc: Descriptor, val meta: ViewMetadata) {
    // reusable state
    val chunk = new Chunk
    val dreader = new SpecificDatumReader[Chunk]

    /**
     * Get the first Path matching the given query.
     * TODO: see issue #7
     */
    def get(query: List[Value]): Option[List[Value]] = {
        val reader = new DataFileReader(desc.filename("data"), dreader)
        try while (reader.hasNext) {
            // read the next chunk
            reader.next(chunk)
            assert(chunk.depth == 0, "TODO: see View.Writer")
            assert(chunk.arity == 3, "TODO: see View.Writer")

            // if any path is >= our path, accept it
            for (path <- chunk.values.grouped(chunk.arity)) {
                if (compare(path, query) >= 0)
                    return Some(path.toList)
            }
        } finally {
            reader.close
        }
        None
    }

    /** TODO: temporary until issue #7 is resolved */
    private def compare(left: Iterable[Value], right: Iterable[Value]): Int =
        compare(left.zipAll(right, EVALUE, NVALUE))
    private def compare(values: Iterable[(Value,Value)]): Int = {
        values match {
            case Nil => 0
            case (EVALUE, _) :: _ => -1
            case (_, NVALUE) :: _ => 1
            case (left, right) :: xs =>
                val c = left.compareTo(right)
                if (c != 0) c else compare(xs)
        }
    }

    override def toString: String = {
        "#<View %d %s>".format(desc.id, meta)
    }
}

