import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = (SparkSession.builder()
                          .getOrCreate())

val data = (spark.read.option("header","true")
                      .option("inferSchema","true")
                      .format("csv")
                      .load("../Data/titanic.csv"))
data.printSchema()

val columnNames = data.columns
val firstRow = data.head(1)(0)
println("")
println("------------Example of data row------------")
for (index <- Range(1, columnNames.length)){
  print(columnNames(index))
  print("       ")
  println(firstRow(index))
}

val logisticRegressionDataComplete = (data.select(data("Survived").as("label"),
                                                  $"Pclass", $"Name", $"Sex",
                                                  $"Age", $"SibSp", $"Parch",
                                                  $"Fare", $"Embarked"))
val logisticRegressionData = logisticRegressionDataComplete.na.drop()