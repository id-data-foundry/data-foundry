name := """DataFoundry"""
organization := "TUe-ID"
version := "0.9.21-SNAPSHOT"
maintainer := "m.funk@tue.nl"

scalaVersion := "2.13.14"

import NativePackagerHelper._
Universal / mappings ++= directory("public")

lazy val root = (project in file("."))
                  .enablePlugins(PlayJava, PlayEbean, SwaggerPlugin)
                  .aggregate(common)
                  .dependsOn(common)

lazy val common = (project in file("modules/common"))
                  .enablePlugins(PlayJava, PlayEbean)


// don't generate documentation
Compile / doc / sources := Seq.empty
Compile / packageDoc / publishArtifact := false

// pack the documentation files for distribution
PlayKeys.externalizeResources := false
PlayKeys.devSettings := Seq("play.pekko.dev-mode.pekko.http.parsing.max-uri-length" -> "20480")

// show out error messages for test
Test / testOptions := Seq(Tests.Argument(TestFrameworks.JUnit, "-a"))
Test / javaOptions += "-Dconfig.resource=tests.conf"

// Java project. Don't expect Scala IDE
EclipseKeys.projectFlavor := EclipseProjectFlavor.Java
// Use .class files instead of generated .scala files for views and routes
EclipseKeys.createSrc := EclipseCreateSrc.ValueSet(EclipseCreateSrc.ManagedClasses, EclipseCreateSrc.ManagedResources)

Universal / javaOptions ++= Seq(
  "-J-Xms512M",
  "-J-Xmx2048M",
  "-J-Xss2M",
  "-J-XX:MaxMetaspaceSize=2048M"
)

// OpenAPI / Swagger configuration
swaggerRoutesFile := "OpenAPIs.routes"
swaggerPrettyJson := true
swaggerDomainNameSpaces := Seq("models")
swaggerNamingStrategy := "snake_case"
