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

println("")
println("Filtering with equal")
df.filter($"High" === 484.40).show() //df.filter("High = 484.40").show() -> Same Result but SQL notation

println("")
println("Saving results in a Scala object")
val CH_low = df.filter($"Close"< 480 && $"High" < 480).collect()

println("")
println("Counting results")
val lowCount = df.filter($"Close"< 480 && $"High" < 480).count()

println("")
println("Correlations")
df.select(corr("High","Low")).show()

