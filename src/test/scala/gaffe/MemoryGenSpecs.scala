
package gaffe

import gaffe.AvroUtils._

import java.nio.ByteBuffer

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class MemoryGenSpecs extends FlatSpec with ShouldMatchers
{
    "A MemoryGen" should "allow individual vertices to be added" in {
        val graph = new MemoryGen(0)
        val oldversion = graph.version
        graph.add(values("src"))
        graph.version should not equal (oldversion)
    }

    it should "allow paths to be added" in {
        val graph = new MemoryGen(0)
        val oldversion = graph.version
        graph.add(values("src", "edge", "dest"))
        graph.version should not equal (oldversion)
    }

    it should "allow exact matches for a vertex" in {
        val graph = new MemoryGen(0)
        graph.add(values("src", "edge", "dest"))
        
        graph.get(Query(values("src"))) should be === (Stream(values("src")))
        graph.get(Query(values("dest"))) should be === (Stream(values("dest")))
    }

    it should "allow exact matches for a path" in {
        val graph = new MemoryGen(0)
        graph.add(values("src", "edge", "dest"))
        
        graph.get(Query(values("src", "edge", "dest"))) should be === (Stream(values("src", "edge", "dest")))
    }

    it should "not match a missing vertex" in {
        val graph = new MemoryGen(0)
        graph.add(values("src", "edge", "dest"))
        
        graph.get(Query(values("not there"))) should be === (Stream.empty)
    }

    it should "fail to match a malformed path" in {
        val graph = new MemoryGen(0)
        graph.add(values("src", "edge", "dest"))
        
        evaluating {graph.get(Query(values("src", "edge")))} should produce [IllegalArgumentException]
    }

    it should "support ordered iteration" in {
        val graph = new MemoryGen(0)
        graph.add(values("apple", "tastes", "sweet"))
        graph.add(values("banana", "tastes", "edgy"))
        
        for (adjacencies <- graph.iterator) yield {
            adjacencies.name
        } should be === (values("apple", "banana", "edgy", "sweet"))
    }
}

