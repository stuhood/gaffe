class Plugins(info: sbt.ProjectInfo) extends sbt.PluginDefinition(info)
{
    // from http://github.com/codahale/avro-sbt : must be published locally
    val avroSBT = "com.codahale" % "avro-sbt" % "0.1.0-SNAPSHOT"
}
