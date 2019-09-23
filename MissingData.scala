import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("Data/ContainsNull.csv")

println("")
println("Printing csv schema")
df.printSchema()

println("")
println("Printing DataFrame")
df.show()