import scala.util.Try

name := "sparkz-core"

lazy val commonSettings = Seq(
  scalaVersion := "2.12.12",
  resolvers += Resolver.sonatypeRepo("public"),
  resolvers += "Maven Central Server" at "https://repo1.maven.org/maven2",
  resolvers += "Typesafe Server" at "https://repo.typesafe.com/typesafe/releases",
  wartremoverErrors ++= Seq(
    Wart.Recursion,
    Wart.TraversableOps,
    Wart.Null,
    Wart.Product,
    Wart.PublicInference,
    Wart.FinalVal,
    Wart.IsInstanceOf,
    Wart.JavaConversions,
    Wart.JavaSerializable,
    Wart.Serializable,
    Wart.OptionPartial),
  organization := "io.horizen",
  organizationName := "Zen Blockchain Foundation",
  version := "2.0.0-RC8",
  licenses := Seq("CC0" -> url("https://creativecommons.org/publicdomain/zero/1.0/legalcode")),
  homepage := Some(url("https://github.com/HorizenOfficial/Sparkz")),
  pomExtra := (
      <scm>
        <url>https://github.com/HorizenOfficial/Sparkz</url>
        <connection>scm:git:git@github.com:HorizenOfficial/Sparkz.git</connection>
      </scm>
      <developers>
        <developer>
          <id>HorizenOfficial</id>
          <name>Zen Blockchain Foundation</name>
          <url>https://github.com/HorizenOfficial</url>
        </developer>
      </developers>
  ),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  publishTo := sonatypePublishToBundle.value,
  fork := true // otherwise, "java.net.SocketException: maximum number of DatagramSockets reached"
)

val circeVersion = "0.14.2"
val akkaVersion = "2.6.19"
val akkaHttpVersion = "10.2.9"

val networkDependencies = Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-parsing" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-protobuf" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.bitlet" % "weupnp" % "0.1.4",
  "commons-net" % "commons-net" % "3.8.0"
)

val apiDependencies = Seq(
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "de.heikoseeberger" %% "akka-http-circe" % "1.39.2"
)

val loggingDependencies = Seq(
  "ch.qos.logback" % "logback-classic" % "1.3.0-alpha16"
)

val scorexUtil = "org.scorexfoundation" %% "scorex-util" % "0.1.6"

val testingDependencies = Seq(
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % "test",
  "org.scalactic" %% "scalactic" % "3.2.12" % "test",
  "org.scalatest" %% "scalatest" % "3.2.12" % "test",
  "org.scalacheck" %% "scalacheck" % "1.16.0",
  "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2" % Test,
  "org.scorexfoundation" %% "scorex-util" % "0.1.6" % Test classifier "tests",
  "org.mockito" % "mockito-scala_2.11" % "0.3.0" % "test"
)

libraryDependencies ++= Seq(
  "com.iheart" %% "ficus" % "1.5.2",
  "org.scorexfoundation" %% "scrypto" % "2.1.7",
  scorexUtil
) ++ networkDependencies ++ apiDependencies ++ loggingDependencies ++ testingDependencies


scalacOptions ++= Seq("-Xfatal-warnings", "-feature", "-deprecation")

javaOptions ++= Seq(
  "-server"
)

testOptions in Test += Tests.Argument("-oD", "-u", "target/test-reports")

pomIncludeRepository := { _ => false }

val credentialFile = Path.userHome / ".ivy2" / ".credentials"
credentials ++= (for {
  file <- if (credentialFile.exists) Some(credentialFile) else None
} yield Credentials(file)).toSeq

lazy val testkit = Project(id = "testkit", base = file(s"testkit"))
  .dependsOn(basics)
  .settings(commonSettings: _*)

lazy val basics = Project(id = "sparkz", base = file("."))
  .settings(commonSettings: _*)

credentials ++= (for {
  username <- Option(System.getenv().get("CONTAINER_OSSRH_JIRA_USERNAME"))
  password <- Option(System.getenv().get("CONTAINER_OSSRH_JIRA_PASSWORD"))
} yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq


// PGP key for signing a release build published to sonatype
// signing is done by sbt-pgp plugin
// how to generate a key - https://central.sonatype.org/pages/working-with-pgp-signatures.html
// how to export a key and use it with Travis - https://docs.scala-lang.org/overviews/contributors/index.html#export-your-pgp-key-pair
pgpPassphrase := sys.env.get("CONTAINER_GPG_PASSPHRASE").map(_.toArray)

//FindBugs settings
findbugsReportType := Some(FindbugsReport.PlainHtml)
