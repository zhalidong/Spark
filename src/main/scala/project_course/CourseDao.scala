package project_course

import HBaseUtils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 数据访问层
  */
object CourseDao {

    val tableName = "course_clickcount"
    val cf = "info"
    val qualifer = "click_count"

    /**
      * 保存数据到hbase
      * @param list  集合
      */
    def save(list:ListBuffer[CourseClickCount])={
        val table: HTable = HBaseUtils.getInstance().getTable(tableName)

        for(ele <- list){
            table.incrementColumnValue(Bytes.toBytes(ele.day_course),
                Bytes.toBytes(cf),
                Bytes.toBytes(qualifer),
                ele.click_count)
        }




    }

    /**
      * 根据rowkey查询值
      * @param day_course
      * @return
      */
    def count(day_course:String):Long={
        val table: HTable = HBaseUtils.getInstance().getTable(tableName)
        val get = new Get(Bytes.toBytes(day_course))
        val value: Array[Byte] = table.get(get).getValue(cf.getBytes,qualifer.getBytes)

        if(null==value){
            0L
        }else{
            Bytes.toLong(value)
        }


    }

    def main(args: Array[String]): Unit = {

        val list = new ListBuffer[CourseClickCount]
        list.append(CourseClickCount("20171111_8",8))
        list.append(CourseClickCount("20171111_9",9))
        list.append(CourseClickCount("20171111_1",100))

        save(list)
        println(count("20171111_1")+":"+count("20171111_8")+":"+count("20171111_9"))

    }

}
