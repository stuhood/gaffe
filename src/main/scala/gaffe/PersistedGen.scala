
package gaffe

import gaffe.io.Chunk

import java.io.{File, FileInputStream}

import scala.collection.JavaConversions.asIterator

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileWriter, DataFileStream}
import org.apache.avro.generic.GenericData
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader}

/**
 * A generation that has been persisted to disk.
 */
object PersistedGen {
    // constants
    val GENERATION_KEY = "generation"
}

class PersistedGen(generation: Long, filename: String) {
    /**
     * Get the first Chunk less than or equal to the given path.
     * TODO: mostly useless
     */
    def chunk(path: List[Value]): Option[(Chunk,Chunk)]= {
        // build a Path object to compare chunks against
        val patho = pathObject(path)

        val rchunk = new Chunk
        val echunk = new Chunk
        val stream = new DataFileStream(new FileInputStream(filename), new SpecificDatumReader[Chunk])
        try {
            while (stream.hasNext) {
                // chunks always come in pairs of 'range' and 'edge'
                stream.next(rchunk)
                stream.next(echunk)
                // if this chunk is <= our path, accept it
                if (patho.compareTo(rchunk.range.begin) >= 0)
                    return Some((rchunk, echunk))
            }
        } finally {
            stream.close
        }
        None
    }

    /**
     * Creates a Path object from a list of values.
     */
    def pathObject(values: List[Value]): Path = {
        if (values.size % 2 == 0) throw new IllegalArgumentException("Invalid path")
        val edgecount = (values.size - 1) / 2

        val path = new Path
        path.source = new Vertex; path.source.name = values.head
        path.edges = new GenericData.Array[Edge](edgecount, Schema.createArray(Edge.SCHEMA$))
        for (List(label, name) <- values.tail.sliding(2)) {
            val edge = new Edge
            edge.label = label
            edge.vertex = new Vertex
            edge.vertex.name = name
            path.edges.add(edge)
        }
        path
    }

    override def toString: String = {
        "#<PersistedGen %s %s>".format(generation, filename)
    }
}

class PersistedGenWriter(generation: Long, filename: String) {
    val writer = {
        val w = new DataFileWriter(new SpecificDatumWriter[Chunk])
        w.setMeta(PersistedGen.GENERATION_KEY, generation)
        w.create(Chunk.SCHEMA$, new File(filename))
    }

    def append(chunk: Chunk) = writer.append(chunk)
    def close() = writer.close
}
