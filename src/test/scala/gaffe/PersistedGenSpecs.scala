
package gaffe

import java.nio.ByteBuffer

import scala.util.Random

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class PersistedGenSpecs extends FlatSpec with ShouldMatchers with Configuration
{
    def values(obs: Any*): List[Value] = {
        for (ob <- obs) yield {
            val value = new Value
            value.value = ByteBuffer.wrap(ob.toString.getBytes)
            value
        }
    }.toList

    // TODO: yea, it needs work
    def filename = "%s/%s".format(config.getString("data_directory").get, new Random().nextLong.abs)

    "A PersistedGenWriter" should "be happy to be empty" in {
        val writer = new PersistedGenWriter(0, filename)
        writer.close
    }
}

