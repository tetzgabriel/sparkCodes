import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("Data/ContainsNull.csv")

println("")
println("Printing csv schema")
df.printSchema()

println("")
println("Printing DataFrame")
df.show()

println("")
println("Removing rows that contains null values")
df.na.drop().show()

println("")
println("Removing rows that not contains at least two non-null values")
df.na.drop(2).show()

println("")
println("Filling integer null values with 100")
df.na.fill(100).show()

println("")
println("Filling string null values with Missing")
df.na.fill("Missing").show()

println("")
println("Filling Name column null values with Missing")
df.na.fill("Missing", Array("Name")).show()
