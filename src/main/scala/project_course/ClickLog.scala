package project_course

/**
  *  数据清洗后的日志信息
  *  ip:日志访问的ip地址
  *  time:日志访问的时间
  *  courseId:日志访问的实战课程编号
  *  statusCode:日志访问的状态码
  *  referer:日志访问的referer
  */
case class ClickLog (ip:String,time:String,courseId:Int,statusCode:Int,referer:String)



