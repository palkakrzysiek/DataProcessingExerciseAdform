package com.kpalka.dataprocessingexercise

import java.io.{ BufferedWriter, File, FileWriter }

import scala.io.Source

object IoOps {
  // not caring about closing the resoucrces for now
  def readCsv(filename: String) = {
    val file                = Source.fromFile(filename)
    val headers #:: content = LazyList.from(file.getLines())
    val headersList         = headers.split(",")
    content
      .map(dataLine => headersList.zip(dataLine.split(",")).toMap)
  }
  def writeLines(filename: String, lines: LazyList[String]) = {
    val file = new File(filename)
    val bw   = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
  }
}
