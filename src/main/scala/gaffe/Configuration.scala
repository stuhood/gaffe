
package gaffe

import java.io.File

import net.lag.configgy.{Config, Configgy, ConfigException}

import org.apache.avro.file.CodecFactory

object Configuration
{
    Configgy.configureFromResource("gaffe.conf", classOf[Configuration].getClassLoader)
    private var config = new Configuration(Configgy.config)

    def apply() = config
}

class Configuration(c: Config) {
    // validation and initialization
    val data_directory = new File(c.getString("data_directory").get)
    if (!data_directory.exists && !data_directory.mkdirs)
        throw new ConfigException("Could not create data_directory.")

    val default_codec = CodecFactory.fromString(c.getString("default_codec", "null"))
}

