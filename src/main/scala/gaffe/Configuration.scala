
package gaffe

import net.lag.configgy.Configgy

object Configuration
{
    Configgy.configureFromResource("gaffe.conf", classOf[Configuration].getClassLoader)
    val config = Configgy.config
}

trait Configuration
{
    val config = Configuration.config
}

