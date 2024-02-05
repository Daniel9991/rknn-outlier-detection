name := "rknn-outlier-detection"

version := "0.1"

scalaVersion := "2.13.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0" % Provided
libraryDependencies += "org.alexboisvert" %% "skiis" % "2.0.2-SNAPSHOT"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.0" % Provided

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${1}_${2}.jar"