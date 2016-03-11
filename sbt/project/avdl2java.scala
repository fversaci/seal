package it.crs4.tools

import java.io.{File, PrintWriter}
import org.apache.avro.compiler.specific.SpecificCompiler

import org.apache.avro.Schema
import org.apache.avro.compiler.idl.Idl
import scala.collection.JavaConversions._

object avdl2java {
  def scanDir(dir : File) : Seq[File] = {
    val all = dir.listFiles
    all.filter(_.isFile) ++ all.filter(_.isDirectory).flatMap(scanDir)
  }

  def makeAvsc(outdir : File) : Seq[File] = {
    outdir.mkdirs
    val parser = new Idl(new File("avro/bdg.avdl"))
    val l = parser.CompilationUnit.getTypes
    l.foreach(schema => {
      val fname = outdir + "/" + schema.getName() + ".avsc"
      val writer = new PrintWriter(new File(fname))
      writer.write(schema.toString)
      writer.close
    })
    scanDir(outdir)
  }

  def run(avdldir : File, baseout : File) : Seq[File] = {
    baseout.mkdirs
    makeAvsc(avdldir)
    // val sFiles = avdldir.listFiles
    val arFile = new File(avdldir.toString + "/AlignmentRecord.avsc")
    SpecificCompiler.compileSchema(arFile, baseout)
    scanDir(baseout)
  }
}
