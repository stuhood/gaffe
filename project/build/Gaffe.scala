
import sbt._

class Gaffe(info: ProjectInfo) extends DefaultProject(info) with avro.AvroCompiler
{
    val scalaToolsSnapshots = "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"
    val lagRepository = "lag.net" at "http://www.lag.net/repo/"

    val avro = "org.apache.hadoop" % "avro" % "1.3.3"
    val configgy  = "net.lag" % "configgy" % "1.5.4"
    val scalatest = "org.scalatest" % "scalatest" % "1.2-for-scala-2.8.0.RC6-SNAPSHOT"

    // clean up avro generated code on clean: default target "src/main/java"
    override def cleanAction = super.cleanAction dependsOn(cleanAvro)
}
