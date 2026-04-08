name := """commons"""
organization := "TUe-ID"
version := "SNAPSHOT"
maintainer := "m.funk@tue.nl"

scalaVersion := "2.13.14"

resolvers += Resolver.bintrayRepo("webjars","maven")
resolvers += "Artifactory" at "https://artifactory.openpreservation.org/artifactory/vera-dev/"
libraryDependencies ++= Seq(
  guice,
  javaJdbc,
  evolutions,
  javaWs,
  caffeine,

  // "com.typesafe.play" % "play-cache_2.13" % "2.9.4",
  "commons-io" % "commons-io" % "2.11.0",
  
  // qr code generation
  "com.google.zxing" % "core" % "3.4.1",
  "com.google.zxing" % "javase" % "3.4.1",
  
  // https://mvnrepository.com/artifact/com.typesafe.play/play-mailer_2.12/8.0.1
  "org.playframework" %% "play-mailer" % "10.0.0",
  "org.playframework" %% "play-mailer-guice" % "10.0.0",
  
  // https://mvnrepository.com/artifact/com.h2database/h2
  "com.h2database" % "h2" % "1.4.199", 

  // https://github.com/atlassian/commonmark-java
  "com.atlassian.commonmark" % "commonmark" % "0.12.1",
  "com.atlassian.commonmark" % "commonmark-ext-gfm-tables" % "0.12.1",

  // https://mvnrepository.com/artifact/org.telegram/telegrambots
  "org.telegram" % "telegrambots" % "5.6.0",  
  // https://mvnrepository.com/artifact/org.telegram/telegrambots-meta
  "org.telegram" % "telegrambots-meta" % "4.9.2",
  // https://mvnrepository.com/artifact/org.telegram/telegrambotsextensions
  "org.telegram" % "telegrambotsextensions" % "4.9.2",
  
  "com.vdurmont" % "emoji-java" % "5.1.1",
  
  // CSV reading
  "com.opencsv" % "opencsv" % "5.1",
  
  // versatile dateparsing
  "com.github.sisyphsu" % "dateparser" % "1.0.4",
  
  // Nashorn JS runner support and sandboxing
  "org.javadelight" % "delight-nashorn-sandbox" % "0.4.2",
  // Nashorn JS needs a standalone instance from JDK 17
  "org.openjdk.nashorn" % "nashorn-core" % "15.7",
  
  // GraalVM for faster JS execution
  "org.graalvm.polyglot" % "polyglot" % "25.0.1",
  
  // deprecated 
  // "org.graalvm.polyglot" % "js-community" % "25.0.1",
  "org.graalvm.polyglot" % "js" % "25.0.1",
  "org.graalvm.js" % "js-scriptengine" % "25.0.1",

  // Graal JS runner support and sandboxing (for now imported in code to fix issues manually)
  // "org.javadelight" % "delight-graaljs-sandbox" % "0.2.0" excludeAll(ExclusionRule(organization = "org.javadelight", name = "delight-nashorn-sandbox")),

  // openAPI
  "org.webjars" %% "webjars-play" % "3.0.1",
  "org.webjars" % "swagger-ui" % "4.15.5",
  
  // // swagger testing with java client
  // "io.swagger.core.v3" % "swagger-annotations" % "2.0.0",
  // "com.squareup.okhttp" % "okhttp" % "2.7.5",
  // "com.squareup.okhttp" % "logging-interceptor" % "2.7.5",
  // "com.google.code.gson" % "gson" % "2.8.1",
  // "io.gsonfire" % "gson-fire" % "1.8.3" % "compile",
  // "org.threeten" % "threetenbp" % "1.3.5" % "compile",
  // "junit" % "junit" % "4.12" % "test",
  // "com.novocode" % "junit-interface" % "0.10" % "test",

  // lucene search engine
  "org.apache.lucene" % "lucene-core" % "9.1.0",
  "org.apache.lucene" % "lucene-queries" % "9.1.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "8.11.1",
  "org.apache.lucene" % "lucene-queryparser" % "9.1.0",

  // RDF and reasoning
  "org.apache.jena" % "jena-core" % "4.5.0",

  // PAC4J --> SSO implementation
  "org.pac4j" %% "play-pac4j" % "12.0.0-PLAY3.0",
  "org.pac4j" % "pac4j-http" % "6.0.0" excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core")),
  "org.pac4j" % "pac4j-oidc" % "6.0.0" excludeAll(ExclusionRule("commons-io" , "commons-io"), ExclusionRule(organization = "com.fasterxml.jackson.core")),

  // https://github.com/yomorun/hashids-java
  "org.hashids" % "hashids" % "1.0.3",

  // Microsoft Email Gateway
  "com.microsoft.graph" % "microsoft-graph" % "5.41.0", //excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"), ExclusionRule(organization = "com.fasterxml.jackson.module"), ExclusionRule(organization = "com.fasterxml.jackson.datatype")),
  "com.azure" % "azure-identity" % "1.2.5", //excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"), ExclusionRule(organization = "com.fasterxml.jackson.module"), ExclusionRule(organization = "com.fasterxml.jackson.datatype")),
  "com.squareup.okhttp3" % "okhttp" % "4.10.0",

  // OpenAI API
  "com.theokanning.openai-gpt3-java" % "service" % "0.11.0" excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"), ExclusionRule(organization = "com.fasterxml.jackson.module"), ExclusionRule(organization = "com.fasterxml.jackson.datatype")),

  // OpenDataLoader for PDF text extraction
  "org.opendataloader" % "opendataloader-pdf-core" % "2.2.0" excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core")),

  // Apache Tika to check file uploads
  "org.apache.tika" % "tika-core" % "3.2.3"

)

// don't generate documentation
// update: https://www.scala-sbt.org/1.x/docs/Migrating-from-sbt-013x.html#slash
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

Assets / pipelineStages := Seq(terser)

// minify js assets
TerserKeys.terserCompress := true
TerserKeys.terserMangle := true
terser / includeFilter := GlobFilter("common*.js") || GlobFilter("local*.js") || GlobFilter("convo*.js")

Universal / javaOptions ++= Seq(
  "-J-Xms512M",
  "-J-Xmx2048M",
  "-J-Xss2M",
  "-J-XX:MaxMetaspaceSize=2048M"
)
