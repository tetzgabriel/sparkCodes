import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv(("../Data/Netflix_2011_2016.csv"))

println("")
println("Printing columns names")
df.columns

println("")
println("Printing schema")
df.printSchema()

println("")
println("Printing first 5 columns")
df.head(5)

println("")
println("Describing dataFrame")
df.describe().show()

println("")
println("Creating new dataFrame with HV Ratio")
val df2 = df.withColumn("HvRatio",(df("High")/df("Volume")))
df2.show()

println("")
println("Printing day with peak in High price")
df.orderBy($"High".desc).show(1)

println("")
println("Printing mean of Close column")
df.select(mean("Close")).show()

println("")
println("Printing max of Volume column")
df.select(max("Volume")).show()

println("")
println("Printing min of Volume column")
df.select(min("Volume")).show()

println("")
println("Printing days with Close minor than 600")
df.filter($"Close" < 600).count()

println("")
println("Printing percentage of High greater than 500")
(df.filter($"High" > 500).count()*1.0/df.count())*100

println("")
println("Printing Pearson correlation between High and Volume")
df.select(corr("High","Volume")).show()

println("")
println("Printing max High per year")
val yearsDf = df.withColumn("Year", year(df("Date")))
val maxYearsDf = yearsDf.select($"Year",$"High").groupBy("Year").max()
maxYearsDf.show()

// What is the average Close for each Calender Month?
val monthsDf = df.withColumn("Month", month(df("date")))
val averageMonthsDf = monthsDf.select($"Close", $"Month").groupBy("Month").mean()
averageMonthsDf.select($"Month", $"avg(Close)").show()
