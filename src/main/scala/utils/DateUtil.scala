package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object DateUtil {

  def  getYesterday(date:String) ={
    val sj = new SimpleDateFormat("yyyy-MM-dd")
    val d: Date = sj.parse(date)
    var cal:Calendar=Calendar.getInstance()
    cal.setTime(d)
    cal.add(Calendar.DATE,-1)
    var yesterday=sj.format(cal.getTime())
    yesterday
  }

  def  getTomorrow(date:String) ={
    val sj = new SimpleDateFormat("yyyy-MM-dd")
    val d: Date = sj.parse(date)
    var cal:Calendar=Calendar.getInstance()
    cal.setTime(d)
    cal.add(Calendar.DATE,1)
    val tomorrow=sj.format(cal.getTime())
    tomorrow
  }

  def  getToday(date:String) ={
    val sj = new SimpleDateFormat("yyyy-MM-dd")
    val today: String = sj.format(new Date(date.toLong))
    today
  }

  def getNextHour(hour:String) ={
    val sj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d: Date = sj.parse(hour)
    var cal:Calendar=Calendar.getInstance()
    cal.setTime(d)
    cal.add(Calendar.HOUR_OF_DAY,12)
    val nextHour=sj.format(cal.getTime())
    nextHour
  }
}
