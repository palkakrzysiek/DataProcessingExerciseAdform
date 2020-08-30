name := "DataProcessingExerciseAdform"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "co.fs2"        %% "fs2-core"  % "2.4.0" // For cats 2 and cats-effect 2
libraryDependencies += "co.fs2"        %% "fs2-io"    % "2.4.0"
libraryDependencies += "org.rogach"    %% "scallop"   % "3.5.1"
