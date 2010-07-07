
package gaffe

import java.io.File

import net.lag.configgy.{Configgy, ConfigException}

object Configuration
{
    Configgy.configureFromResource("gaffe.conf", classOf[Configuration].getClassLoader)
    val config = Configgy.config

    // validation and initialization
    {
        val data_directory = new File(config.getString("data_directory").get)
        if (!data_directory.exists && !data_directory.mkdirs)
            throw new ConfigException("Could not create data_directory.")
    }
}

trait Configuration
{
    val config = Configuration.config
}

