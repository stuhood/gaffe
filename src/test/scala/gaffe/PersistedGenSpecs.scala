
package gaffe

import gaffe.AvroUtils._
import gaffe.io.Range
import gaffe.PersistedGen._

import java.io.File
import java.nio.ByteBuffer

import scala.util.Random

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class PersistedGenSpecs extends FlatSpec with ShouldMatchers with Configuration
{
    /**
     * Write a PersistedGen containing the given paths.
     */
    def write(paths: Seq[List[Value]]): PersistedGen = {
        val gen = new Random().nextLong.abs
        // store paths in a MemoryGen
        val memgen = new MemoryGen(gen)
        for (path <- paths) {
            assert(path.size == 3, "FIXME: assuming paths of length 1: " + path)
            memgen.add(path)
        }
        write(memgen)
    }

    def write(memgen: MemoryGen): PersistedGen = {
        // write as a PersistedGen
        val gen = memgen.generation
        val dir = new File(config.getString("data_directory").get)
        val desc = PersistedGen.Descriptor(gen, dir)
        val metas = View.metadata(gen, 0, 1, false, false) ::
            View.metadata(gen, 1, 1, true, false) :: Nil
        new PersistedGen.Writer(desc, metas).write(memgen)
    }

    "A PersistedGen" should "be happy to be empty" in {
        val reader = write(List())
        // check that all views are empty
        for (view <- reader.views)
            view.chunk(mkpath("blah")) should equal (None)
    }

    it should "not mind containing a few chunks either" in {
        val reader = write(List(values("pelican", "eats", "trout")))
        val outbound = reader.viewsFor(inverted = false).head
        outbound.chunk(mkpath("pelican", "eats", "trout")) should not equal (None)
        val inbound = reader.viewsFor(inverted = false).head
        inbound.chunk(mkpath("trout", "eats", "pelican")) should not equal (None)
    }
}

