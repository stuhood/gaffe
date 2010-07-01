
import sbt._

class Gaffe(info: ProjectInfo) extends DefaultProject(info)
{
    val avro = "org.apache.hadoop" % "avro" % "1.3.3"
}
