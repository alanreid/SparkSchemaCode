package ar.com.alanreid.schemacode

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame;
import ar.com.alanreid.schemacode._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.language.implicitConversions


object SchemaCodeDriver extends App {

  implicit def schemaAsCode(dataframe: DataFrame) = new SchemaAsCode(dataframe)

  if(args.length != 3) {
    println("Usage: SchemaCodeDriver <className> <inputFile> <outputFile>")
    sys.exit(1)
  }

  val className = args(0)
  val inputFile = args(1)
  val outputFile = args(2)

  val conf = new SparkConf()
  conf.setAppName("SchemaCodeDriver")
  
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.jsonFile(inputFile)
  val code = df.schemaAsCode(className)
  
  Files.write(Paths.get(outputFile), code.getBytes(StandardCharsets.UTF_8))
}
