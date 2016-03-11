package it.crs4.tools

import java.io.File
import org.apache.avro.compiler.specific.SpecificCompiler

import org.apache.avro.Schema
import org.apache.avro.compiler.idl.Idl
import scala.collection.JavaConversions._

object avsc2java {
  def scanDir(dir : File) : Seq[File] = {
    val all = dir.listFiles
    all.filter(_.isFile) ++ all.filter(_.isDirectory).flatMap(scanDir)
  }

  def run(base : File) : Seq[File] = {
    base.mkdirs
    val sFiles = new File("avro-schemas").listFiles
    SpecificCompiler.compileSchema(sFiles, base)
    scanDir(base)
  }
}
