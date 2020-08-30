package com.kpalka.dataprocessingexercise

import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val viewsFile              = opt[String](default = Some("Views.csv"))
  val clicksFile             = opt[String](default = Some("Clicks.csv"))
  val viewableViewEventsFile = opt[String](default = Some("ViewableViewEvents.csv"))
  val viewsWithClicksOutput  = opt[String](default = Some("ViewsWithClicks.csv"))
  val viewableViewsOutput    = opt[String](default = Some("ViewableViews.csv"))
  val statisticsOutput       = opt[String](default = Some("statistics.csv"))
  verify()
}
