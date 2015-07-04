package ar.com.alanreid.schemacode

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.{ StructField, StructType, ArrayType }


class SchemaAsCode(df: DataFrame) {

  def schemaAsCode(): String = {
    asCode(df.rdd.sparkContext.appName, df.schema.fields)
  }

  def schemaAsCode(className: String): String = {
    asCode(className, df.schema.fields)
  }

  private def asCode(className: String, fields: Array[StructField]): String = {

    def normalizeClassName(name: String): String = {
      name.split("_").map(_.capitalize).mkString("").split(" ").map(_.capitalize).mkString("")
    }

    def handleArrayOfArrays(field: ArrayType): String = {
      field match {
        case ArrayType(f: ArrayType, _) => "Array[" + handleArrayOfArrays(f) + "]"
        case ArrayType(f, _) => normalizeClassName(f.typeName)
      }
    }

    def extractFields(field: StructField): String = {
      field.dataType match {
        
        // Reference to a generated class
        case StructType(f: Array[StructField]) => "`" + field.name + "`: " + normalizeClassName(className) + normalizeClassName(field.name)
        
        // Reference to an array of a generated item class
        case ArrayType(f: StructType, _) => "`" + field.name + "`: Array[" + normalizeClassName(className) + normalizeClassName(field.name) + "Item" + "]"

        // Multidimensional arrays
        case ArrayType(f: ArrayType, _) => "`" + field.name + "`: Array[" + handleArrayOfArrays(f) + "]"

        // Simple arrays
        case ArrayType(f,_) => "`" + field.name + "`: Array[" + normalizeClassName(f.typeName) + "]"

        // The dark and scary rest (any other types)
        case _ => "`" + field.name + "`: " + normalizeClassName(field.dataType.typeName)
      }
    }

    def extractClasses(field: StructField): String = {
      field.dataType match {

        // Generate item case class
        case ArrayType(f: StructType, _) => asCode(normalizeClassName(className) + normalizeClassName(field.name) + "Item", f.fields)
        
        // Generate case class
        case StructType(f: Array[StructField]) => asCode(normalizeClassName(className) + normalizeClassName(field.name), f)
        
        case _ => null
      }
    }

    val list = fields.map(extractFields) collect { 
      case item: String => item 
    }

    val classes = fields.map(extractClasses) collect { 
      case item: String => item
    }

    "\ncase class " + normalizeClassName(className) + "(" + list.mkString(", ") + ")" + classes.mkString("")
  }

}
