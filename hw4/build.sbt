organization := "com.typesafe.akka.samples"
name := "akka-sample-fsm-java"

val akkaVersion = "2.5.22"

scalaVersion := "2.12.8"
libraryDependencies ++= Seq(
  "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % "1.0.1",
  "com.typesafe.akka" %% "akka-discovery" % "2.5.21",
  "com.typesafe.akka" %% "akka-slf4j"   % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %%       "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion
)

