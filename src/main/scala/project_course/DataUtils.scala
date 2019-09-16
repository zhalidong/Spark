package project_course

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


/**
  * 日期时间工具类
  * 2019-09-16 10:22:01
  * 20190916102201
  */
object DataUtils {
    val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")


    def getTime(time:String)={
        YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
    }

    def parseToMinute(time:String)={
        TARGE_FORMAT.format(new Date(getTime(time)))
    }

    def main(args: Array[String]): Unit = {
        println(parseToMinute("2019-09-16 10:22:01"))
    }


}
