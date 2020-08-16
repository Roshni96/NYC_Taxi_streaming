libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
val circeVersion = "0.9.0"
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)