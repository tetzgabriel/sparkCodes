import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv(("Data/CitiGroup2006_2008"))

println("")
println("Printing csv schema")
df.printSchema()

println("")
println("Filtering using scala NoSQL notation")
df.filter("Close > 480").show() //df.filter($"Close" > 480).show() -> Same Result but SQL notation

println("")
println("Using multiple filters")
df.filter($"Close"< 480 && $"High" < 480).show() //df.filter("Close < 480 AND High < 480") -> Same Result but SQL notation