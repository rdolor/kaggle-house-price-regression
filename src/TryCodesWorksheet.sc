
//import org.apache.spark.sql.SparkSession
//
//val spark = SparkSession
//  .builder()
//  .appName("Spark SQL basic example")
//  .config("spark.some.config.option", "some-value")
//  .getOrCreate()
//
//// For implicit conversions like converting RDDs to DataFrames
//import spark.implicits._


//def square(x: Int) = x * x
//square(2)

case class Person(name: String, age: Long)

// Encoders are created for case classes
val caseClassDS = Seq(Person("Andy", 32)).toDS()

caseClassDS.show()