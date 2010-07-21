
package gaffe

import gaffe.AvroUtils._

import java.nio.ByteBuffer

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class MemoryGenSpecs extends FlatSpec with ShouldMatchers
{
    def memgen(paths: List[Value]*): MemoryGen = {
        val graph = new MemoryGen(0)
        for (path <- paths) graph.add(path)
        graph
    }

    "A MemoryGen" should "allow exact matches for a vertex" in {
        val graph = memgen(values("src", "edge", "dest"))
        graph.get(Query(values("src"))) should be === (Stream(values("src")))
        graph.get(Query(values("dest"))) should be === (Stream(values("dest")))
    }

    it should "allow exact matches for a path" in {
        val graph = memgen(values("src", "edge", "dest"))
        graph.get(Query(values("src", "edge", "dest"))) should be === (Stream(values("src", "edge", "dest")))
    }

    it should "not match a missing vertex" in {
        val graph = memgen(values("src", "edge", "dest"))
        graph.get(Query(values("not there"))) should be === (Stream.empty)
    }

    it should "fail to match a malformed path" in {
        val graph = memgen(values("src", "edge", "dest"))
        evaluating {graph.get(Query(values("src", "edge")))} should produce [IllegalArgumentException]
    }

    it should "support ordered iteration" in {
        val graph = memgen(values("apple", "tastes", "sweet"),
            values("banana", "tastes", "edgy"))
        
        graph.outboundIterator.map(_.src).toList should equal (values("apple", "banana"))
        graph.inboundIterator.map(_.dest).toList should equal (values("edgy", "sweet"))
    }

    "A Query against a MemoryGen" should "support Identity clauses" in {
        val graph = memgen(values("alpha", "1st", "term"),
            values("beta", "2nd", "term"),
            values("gaga", "3rd", "term"))
        
        graph.get(Query("beta", null, "term")) should be ===
            (Stream(values("beta", "2nd", "term")))
    }

    it should "support Range clauses" in {
        val graph = memgen(values("alpha", "1st", "term"),
            values("beta", "2nd", "term"),
            values("gaga", "3rd", "term"))
        
        graph.get(Query(("alpha", "gaga"), ("", null), "term")) should be ===
            (Stream(values("alpha", "1st", "term"), values("beta", "2nd", "term")))
        graph.get(Query(null, ("2nd", "3rd"), "term")) should be ===
            (Stream(values("beta", "2nd", "term"), values("gaga", "3rd", "term")))
    }

    it should "support List clauses" in {
        val graph = memgen(values("alpha", "1st", "term"),
            values("beta", "2nd", "term"),
            values("gaga", "3rd", "term"))
        
        graph.get(Query(List("alpha", "gaga"), null, "term")) should be ===
            (Stream(values("alpha", "1st", "term"), values("gaga", "3rd", "term")))
        graph.get(Query(null, List("2nd"), "term")) should be ===
            (Stream(values("beta", "2nd", "term")))
    }
}

