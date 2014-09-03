import Dependencies._

name := "common-pipeline"

version := "2014.09.03-0-SNAPSHOT"

libraryDependencies ++= Seq(sprayJson,
		    awsJavaSdk exclude ("com.fasterxml.jackson.core", "jackson-annotations"),
		    commonsIO)
 
