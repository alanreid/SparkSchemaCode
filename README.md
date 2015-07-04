# Spark SchemaCode
SchemaCode is an **experimental** utility to generate Scala classes from a Spark DataFrame's schema

## Using SchemaCode
**Installing the required tools**
```bash
$ brew install sbt apache-spark
```

**Compiling**
```
sbt assembly
```

**Using SchemaCode with Spark-Shell**
```
spark-shell --jars target/scala-2.10/schemacode-assembly-0.1.0.jar
```

**Driver Parameters:**
```
SchemaCodeDriver <className> <inputfile> <outputfile>
```

**Running SchemaCodeDriver:**
```
spark-submit --master 'local[*]' \
--class ar.com.alanreid.schemacode.SchemaCodeDriver \
target/scala-2.11/schemacode-assembly-0.1.0.jar \
Tweet data/tweets_sample.json data/schema.scala
```

### Disclaimer
This is an experimental and unstable library.

### Licence
This software is distributed under the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0
