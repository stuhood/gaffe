
package gaffe

import gaffe.io.Range
import gaffe.PersistedGen._
import gaffe.AvroUtils._

import java.io.File
import java.nio.ByteBuffer

import scala.util.Random

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class PersistedGenSpecs extends FlatSpec with ShouldMatchers with Configuration
{
    /**
     * Write a PersistedGen with a single View containing the given paths.
     * TODO: dumb in multiple ways
     */
    def write(paths: List[Path]): PersistedGen = {
        val dir = new File(config.getString("data_directory").get)
        val gen = new Random().nextLong.abs
        val id = 0
        val desc = View.Descriptor(id, PersistedGen.Descriptor(gen, dir))
        val meta = View.metadata(gen, id, 1, false, false)
        val writer = new ViewWriter(desc, meta)
        for (path <- paths) {
            // TODO: should replace with 1. write MemoryGen, 2. flush to PersistedGen
            assert(path.edges.size == 1, "FIXME: assuming paths of length 1: " + path)
            val range = new Range
            range.begin = path
            range.end = mkpath(null)
            val edges = genarray(Edge.SCHEMA$, path.edges.iterator.next)

            writer.append(range, edges)
        }
        writer.close
        // open for reading
        new PersistedGen(desc.gen, new View(desc)::Nil)
    }

    "A PersistedGen" should "be happy to be empty" in {
        val reader = write(List())
        reader.views.head.chunk(mkpath("blah", "blah", "blah")) should equal (None)
    }

    it should "not mind containing a few chunks either" in {
        val reader = write(List(mkpath("pelican", "eats", "trout")))
        reader.views.head.chunk(mkpath("pelican", "eats", "trout")) should not equal (None)
    }
}

