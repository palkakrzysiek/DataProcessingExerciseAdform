package com.kpalka.dataprocessingexercise

import java.time.{Duration, LocalDateTime}
import WindowedElements._


object Processors {
  private val size = Duration.ofSeconds(5)
  private val slide = Duration.ofSeconds(5)
//  private case class TimeWindowState(lastTimestamp: Option[LocalDateTime], acc: List[ViewWithClick])
//  private case object ZERO extends TimeWindowState(None, List.empty)


//  def viewsWithClicks(views: LazyList[View], clicks: LazyList[Click]): LazyList[ViewWithClick] = {
//    views.joinUsingSlidingWindows(clicks)(_.logTime, _.logTime, size, slide, _.id == _.campaignId)
//
////    views.scanLeft(ZERO) { (windowState, view) =>
////
////    }
//  }

}
