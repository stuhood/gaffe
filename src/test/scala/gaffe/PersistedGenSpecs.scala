
package gaffe

import gaffe.io.Range
import gaffe.PersistedGen._
import gaffe.AvroUtils._

import java.nio.ByteBuffer

import scala.util.Random

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class PersistedGenSpecs extends FlatSpec with ShouldMatchers with Configuration
{
    /**
     * Write a PersistedGen containing the given paths.
     * TODO: Assumes paths of length 1
     */
    def write(paths: List[Path]): PersistedGen = {
        val name = "%s/%s".format(config.getString("data_directory").get, new Random().nextLong.abs)
        val writer = new PersistedGenWriter(0, name)
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
        new PersistedGen(0, name)
    }

    "A PersistedGen" should "be happy to be empty" in {
        val reader = write(List())
        reader.chunk(mkpath("blah", "blah", "blah")) should equal (None)
    }

    it should "not mind containing a few chunks either" in {
        val reader = write(List(mkpath("pelican", "eats", "trout")))
        reader.chunk(mkpath("pelican", "eats", "trout")) should not equal (None)
    }
}

