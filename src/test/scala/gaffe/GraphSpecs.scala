
package gaffe

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class GraphSpecs extends FlatSpec with ShouldMatchers
{
    "Graphs" should "increment their version when modified" in {
        val graph = new Graph(0)
        val oldversion = graph.version

        val src = new Vertex
        src.name = new Value

        val path = new Path
        path.source = new Vertex
        path.source.name = new Value
        path.source.name.value = ByteBuffer.wrap("src".getBytes)
        path.edges = new GenericData.Array[Edge](1, Schema.createArray(Edge.SCHEMA$))
        val edge = new Edge
        edge.label = new Value
        edge.label.value = ByteBuffer.wrap("edge".getBytes)
        edge.dest = new Vertex
        edge.dest.name = new Value
        edge.dest.name.value = ByteBuffer.wrap("dest".getBytes)
        path.edges.add(edge)

        graph.add(path)
        graph.version should not equal (oldversion)
    }
}

