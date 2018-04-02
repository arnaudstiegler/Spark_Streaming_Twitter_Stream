name := "sparkStreaming"

version := "1.0"

scalaVersion := "2.11.5"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.5.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.5.2"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark" % "2.1.0"

