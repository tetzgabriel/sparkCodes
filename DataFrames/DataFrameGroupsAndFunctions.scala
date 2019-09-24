import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("../Data/Sales.csv")

println("")
println("Printing csv schema")
df.printSchema()

println("")
println("Printing DataFrame")
df.show()


println("")
println("Printing columns average of companies")
df.groupBy("Company").mean().show()

println("")
println("Counting employees by company")
df.groupBy("Company").count().show()

println("")
println("Showing max value of a company")
df.groupBy("Company").max().show()

println("")
println("Showing min value of a company")
df.groupBy("Company").min().show()

println("")
println("Sum of values of a company")
df.groupBy("Company").sum().show()

println("")
println("Ordering results by an ascending field")
df.orderBy("Sales").show()

println("")
println("Ordering results by a descending field")
df.orderBy($"Sales".desc).show()

