import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

println("")
println("Loading Data")
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("../Data/Clean-USA-Housing.csv")

println("")
println("Printing schema")
data.printSchema()

println("")
println("Printing example of data row")
val columnNames = data.columns
val firstRow = data.head(1)(0)
for(index <- Range(1, columnNames.length)){
  println(columnNames(index))
  println(firstRow(index))
  println("")
}

println("")
println("Creating dataFrame with \"label\",\"features\" appearance")
val df = (data.select(data("Price").as("label"),
          $"Avg Area Income", $"Avg Area House Age",
          $"Avg Area Number of Rooms", $"Avg Area Number of Bedrooms",
          $"Area Population"))

val assembler = (new VectorAssembler().setInputCols(Array("Avg Area Income", "Avg Area House Age",
                                                          "Avg Area Number of Rooms", "Avg Area Number of Bedrooms",
                                                          "Area Population"))
                                      .setOutputCol("features"))
val output = assembler.transform(df).select($"label",$"features")
output.show()

println("")
println("Creating linear regression model")
val lr = new LinearRegression()
val lrModel = lr.fit(output)
val trainingSummary = lrModel.summary

println("")
println("Printing residuals")
trainingSummary.residuals.show()