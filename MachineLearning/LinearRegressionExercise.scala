import org.apache.log4j._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

Logger.getLogger("org").setLevel(Level.ERROR)

println("")
println("Creating spark session")
val spark = SparkSession.builder().getOrCreate()

println("")
println("Reading file")
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("../Data/Clean-Ecommerce.csv")

println("")
println("Printing schema")
data.printSchema()

println("")
println("Print example row")
val columnNames = data.columns
val firstRow = data.head(1)(0)
for(index <- Range(1, columnNames.length)){
  println(columnNames(index))
  println(firstRow(index))
  println("")
}

// Rename the Yearly Amount Spent Column as "label"
// Also grab only the numerical columns from the data
// Set all of this as a new dataframe called df
println("")
println("Putting in label/features model")
val df = (data.select(data("Yearly Amount Spent").as("label"),
                      $"Avg Session Length", $"Time on App",
                      $"Time on Website", $"Length of Membership"))

val assembler = (new VectorAssembler().setInputCols(Array("Avg Session Length", "Time on App",
                                                          "Time on Website", "Length of Membership"))
                                      .setOutputCol("features"))
val output = assembler.transform(df).select($"label",$"features")
output.show()

println("")
println("Creating linear regression object")
val lr = new LinearRegression()

println("")
println("Fitting model")
val lrModel = lr.fit(output)

println("")
println("Summaring the results")
val trainingSummary = lrModel.summary

println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError} MSE: ${trainingSummary.meanSquaredError} r2: ${trainingSummary.r2}")