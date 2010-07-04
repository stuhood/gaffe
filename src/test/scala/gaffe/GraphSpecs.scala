
package gaffe

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class GraphSpecs extends FlatSpec with ShouldMatchers
{
    "A Graph" should "allow individual vertices to be added" in {
        val graph = new Graph(0)
        val oldversion = graph.version

        val src = new Value
        src.value = ByteBuffer.wrap("src".getBytes)

        graph.add(src)
        graph.version should not equal (oldversion)
    }

    it should "allow paths to be added" in {
        val graph = new Graph(0)
        val oldversion = graph.version

        val src = new Value
        src.value = ByteBuffer.wrap("src".getBytes)
        val edge = new Value
        edge.value = ByteBuffer.wrap("edge".getBytes)
        val dest = new Value
        dest.value = ByteBuffer.wrap("dest".getBytes)

        graph.add(src, edge, dest)
        graph.version should not equal (oldversion)
    }
}

