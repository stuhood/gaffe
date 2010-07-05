
package gaffe

import scala.collection.mutable.WrappedArrayBuilder

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class GraphSpecs extends FlatSpec with ShouldMatchers
{
    def values(strings: String*) = for (string <- strings) yield {
        val value = new Value
        value.value = ByteBuffer.wrap(string.getBytes)
        value
    }

    // implicitly convert values to optional values
    implicit def vals2optvals(vals: Seq[Value]) = vals.map(Option(_))

    "A Graph" should "allow individual vertices to be added" in {
        val graph = new Graph(0)
        val oldversion = graph.version
        graph.add(values("src"): _*)
        graph.version should not equal (oldversion)
    }

    it should "allow paths to be added" in {
        val graph = new Graph(0)
        val oldversion = graph.version
        graph.add(values("src", "edge", "dest"): _*)
        graph.version should not equal (oldversion)
    }

    it should "allow exact matches for paths" in {
        val graph = new Graph(0)
        graph.add(values("src", "edge", "dest"): _*)
        
        val iter = graph.get(values("src", "edge", "dest"): _*)
        iter.hasNext should be (true)
    }
}

