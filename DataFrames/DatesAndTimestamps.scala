import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header","true").option("inferSchema", "true").csv("../Data/CitiGroup2006_2008")

println("")
println("Printing csv schema")
df.printSchema()

println("")
println("Printing DataFrame")
df.show()

println("")
println("Printing months of the rows")
df.select(month(df("date"))).show()

println("")
println("Printing years of the rows")
df.select(year(df("date"))).show()

println("")
println("Printing new DataFrame with Close average grouped by Years")
val df2 = df.withColumn("Year",year(df("Date"))) //Create new df with Column Year
val dfAverages = df2.groupBy("Year").mean() //Create new df with the mean grouped by year
dfAverages.select($"Year",$"avg(Close)").show()

println("")
println("Printing new DataFrame with min price grouped by Years")
val df3 = df.withColumn("Year",year(df("Date"))) //Create new df with Column Year
val dfMinimun = df2.groupBy("Year").min() //Create new df with the mean grouped by year
dfMinimun.select($"Year",$"min(Close)").show()