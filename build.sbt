name := "rknn-outlier-detection"

version := "0.1"

scalaVersion := "2.13.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.alexboisvert" %% "skiis" % "2.0.2-SNAPSHOT"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test