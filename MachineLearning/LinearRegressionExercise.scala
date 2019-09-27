import org.apache.log4j._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()
// Use Spark to read in the Ecommerce Customers csv file.
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("../Data/Clean-Ecommerce.csv")
// Print the Schema of the DataFrame
data.printSchema()

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
val df = (data.select(data("Yearly Amount Spent").as("label"),
                      $"Avg Session Length", $"Time on App",
                      $"Time on Website", $"Length of Membership"))

val assembler = (new VectorAssembler().setInputCols(Array("Avg Session Length", "Time on App",
                                                          "Time on Website", "Length of Membership"))
                                      .setOutputCol("features"))
val output = assembler.transform(df).select($"label",$"features")
output.show()

val lr = new LinearRegression()
val lrModel = lr.fit(output)
val trainingSummary = lrModel.summary

println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError} MSE: ${trainingSummary.meanSquaredError} r2: ${trainingSummary.r2}")