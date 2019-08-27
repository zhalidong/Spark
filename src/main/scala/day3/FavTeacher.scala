package day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据文件
  * http://bigdata.edu360.cn/laozhang
    http://bigdata.edu360.cn/laozhang
    http://bigdata.edu360.cn/laozhao
    http://bigdata.edu360.cn/laozhao
  *
  * 作业，求最受欢迎的老师
  *1.在所有的老师中求出最受欢迎的老师Top3
  *
  */
object FavTeacher {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
        val sc = new SparkContext(conf)

        val lines: RDD[String] = sc.textFile(args(0))
        //整理数据
        val teacherAndOne = lines.map(line => {
            val index = line.lastIndexOf("/")
            val teacher = line.substring(index + 1)
            //            val httpHost = line.substring(0,index)
            //            val subject = new URL(httpHost).getHost.split("[.]")(0)
            (teacher, 1)
        })


        //聚合
        val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey((x,y)=>x+y)
        //排序
        val sorted: RDD[(String, Int)] = reduced.sortBy(_._2,false)
        //触发Action执行计算
        val result: Array[(String, Int)] = sorted.collect()


        //打印
        println(result.toBuffer)

        sc.stop()

    }

}
