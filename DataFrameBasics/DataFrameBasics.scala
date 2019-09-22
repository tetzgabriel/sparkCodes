import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header","true").option("inferSchema", "true").csv("../SparkDataFrames/CitiGroup2006_2008")

println("")
println("Printing DataFrame")
for (row <- df.head(5)){
  println(row)
}

println("")
println("Printing Columns")
df.columns

println("")
println("Describing the DataFrame")
df.describe().show()

println("")
println("Printing only one column")
df.select("Volume").show()

println("")
println("Printing more columns")
df.select($"Date", $"Close").show()

println("")
println("Creating one more column")
val df2 = df.withColumn("High+Low",df("High")+df("Low"))
df2.show()

println("")
println("Printing df2 schema")
df2.printSchema()

println("")
println("Changing column name for printing")
df2.select($"High+Low".as("HpL"),$"Close").show()
