
package gaffe

import gaffe.io.{Chunk, Range}
import gaffe.PersistedGen._

import scala.collection.JavaConversions.asIterator

import java.io.File

import org.apache.avro.file.{DataFileWriter, DataFileStream}
import org.apache.avro.generic.GenericArray
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader}

/**
 * A generation that has been persisted to disk as one or more 'Views' of the same base data.
 */
class PersistedGen(descriptor: Descriptor, val views: List[View]) {
    override def toString: String = {
        "#<PersistedGen %s %s>".format(descriptor, views)
    }
}

object PersistedGen {
    // handle for a PersistedGen
    case class Descriptor(generation: Long, directory: File)
}
