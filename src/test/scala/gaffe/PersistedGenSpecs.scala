
package gaffe

import gaffe.AvroUtils._
import gaffe.PersistedGen._

import java.io.File
import java.nio.ByteBuffer

import scala.util.Random

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class PersistedGenSpecs extends FlatSpec with ShouldMatchers
{
    /**
     * Write a PersistedGen containing the given paths.
     */
    def write(paths: List[Value]*): PersistedGen = {
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
        val desc = PersistedGen.Descriptor(gen, Configuration().data_directory)
        val metas = View.metadata(gen, 0, 1, false, false) ::
            View.metadata(gen, 1, 1, true, false) :: Nil
        new PersistedGen.Writer(desc, metas).write(memgen)
    }

    "A PersistedGen" should "be happy to be empty" in {
        val reader = write()
        // check that all views are empty
        for (view <- reader.views)
            view.get(values("blah")) should equal (None)
    }

    it should "not mind containing a few chunks either" in {
        val reader = write(values("pelican", "eats", "trout"),
            values("shark", "eats", "human"))
        val outbound = reader.viewsFor(inverted = false).head
        outbound.get(values("pelican", "eats", "trout")) should be ===
            (Some(values("pelican", "eats", "trout")))
        val inbound = reader.viewsFor(inverted = true).head
        inbound.get(values("trout", "eats", "pelican")) should be ===
            (Some(values("trout", "eats", "pelican")))
    }

    /*
    it should "be fast" in {
        var start = System.currentTimeMillis
        // store paths in a MemoryGen
        val memgen = new MemoryGen(new Random().nextLong.abs)
        for (x <- 0 until 1000; y <- 1000 until 2000) {
            memgen.add(values(x, "loves", y))
        }
        println("Added in " + (System.currentTimeMillis - start))

        System.gc
        Thread.sleep(5000)

        start = System.currentTimeMillis
        val reader = write(memgen)
        println("Flushed in " + (System.currentTimeMillis - start))
    }
    */
}

