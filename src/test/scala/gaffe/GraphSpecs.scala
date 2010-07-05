
package gaffe

import scala.collection.mutable.WrappedArrayBuilder

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class GraphSpecs extends FlatSpec with ShouldMatchers
{
    def values(strings: String*): List[Value] = {
        for (string <- strings) yield {
            val value = new Value
            value.value = ByteBuffer.wrap(string.getBytes)
            value
        }
    }.toList

    "A Graph" should "allow individual vertices to be added" in {
        val graph = new Graph(0)
        val oldversion = graph.version
        graph.add(values("src"))
        graph.version should not equal (oldversion)
    }

    it should "allow paths to be added" in {
        val graph = new Graph(0)
        val oldversion = graph.version
        graph.add(values("src", "edge", "dest"))
        graph.version should not equal (oldversion)
    }

    it should "allow exact matches for a vertex" in {
        val graph = new Graph(0)
        graph.add(values("src", "edge", "dest"))
        
        graph.get(values("src")) should be === (values("src"))
        graph.get(values("dest")) should be === (values("dest"))
    }

    it should "allow exact matches for a path" in {
        val graph = new Graph(0)
        graph.add(values("src", "edge", "dest"))
        
        graph.get(values("src", "edge", "dest")) should be === (values("src", "edge", "dest"))
    }

    it should "fail to match a missing vertex" in {
        val graph = new Graph(0)
        graph.add(values("src", "edge", "dest"))
        
        evaluating {graph.get(values("not there"))} should produce [RuntimeException]
    }
}

