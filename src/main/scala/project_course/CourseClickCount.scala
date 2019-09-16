package project_course

/**
  * 实战课程点击数 实体类
  * @param day_course 对应的就是hbase中的 20171111_1
  * @param click_count 点击总数
  */
case class CourseClickCount(day_course:String,click_count:Long)

