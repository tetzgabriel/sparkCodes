import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

def main(): Unit = {
  println("")
  println("Creating spark session")
  val spark = SparkSession.builder().appName("LinearRegressionExample").getOrCreate()

  println("")
  println("Creating path to archive")
  val path = "../Data/sample_linear_regression_data.txt"

  println("")
  println("Training data")
  val training = spark.read.format("libsvm").load(path)
  training.printSchema()

  println("")
  println("Creating linear regression object")
  val lr = new LinearRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8)

  println("")
  println("Fitting the model")
  val lrModel = lr.fit(training)

  println("")
  println("Printing coefficients and intercept")
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  println("")
  println("Summarizing the model")
  val trainingSummary = lrModel.summary
  println(s"numIterations: ${trainingSummary.totalIterations}")
  println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
  trainingSummary.residuals.show()
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")

  println("")
  println("Stoping spark")
  spark.stop()
}
main()
