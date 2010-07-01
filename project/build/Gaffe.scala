
import sbt._

class Gaffe(info: ProjectInfo) extends DefaultProject(info) with avro.AvroCompiler
{
    val avro = "org.apache.hadoop" % "avro" % "1.3.3"

    // clean up avro generated code on clean: default target "src/main/java"
    override def cleanAction = super.cleanAction dependsOn(cleanAvro)
}
