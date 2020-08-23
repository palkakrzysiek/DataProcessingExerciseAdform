package com.kpalka

import java.time.LocalDateTime

package object dataprocessingexercise {
  case class View(id: Long, logTime: LocalDateTime, campaignId: Long)
  case class Click(id: Long, logTime: LocalDateTime, campaignId: Long, interactionId: Long)
  case class ViewableViewEvent(id: Long, logTime: LocalDateTime, interactionId: Long)
  case class ViewWithClick(id: Long, logTime: LocalDateTime, clickId: Long)
  case class Statistics(campaignId: Long, views: Long, clicks: Long, viewableViews: Long, clickThroughRate: Double)
}
