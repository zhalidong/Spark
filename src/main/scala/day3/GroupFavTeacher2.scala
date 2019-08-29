package day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *2.求每个学科中最受欢迎老师的top3（至少用2到三种方式实现）
  *    http://bigdata.edu360.cn/laozhao
  */
object GroupFavTeacher2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("GroupFavTeacher2").setMaster("local[4]")
        val sc = new SparkContext(conf)

        val N =args(1).toInt

        val subjects = Array("bigdata","javaee","php")

        val lines: RDD[String] = sc.textFile(args(0))
        //整理数据
        val subjectAndteacher:RDD[((String,String),Int)] = lines.map(line => {
            val index = line.lastIndexOf("/")
            val teacher = line.substring(index + 1)
            val httpHost = line.substring(0,index)
            val subject = new URL(httpHost).getHost.split("[.]")(0)
            ((subject, teacher),1)
        })

        //和一组合在一起 不好 调用了两次map方法
        //val map: Any => RDD[Nothing] = subjectAndteacher.map(_,1)

        //聚合 将学科和老师联合当做key
        val reduced: RDD[((String, String), Int)] = subjectAndteacher.reduceByKey(_+_)




        //scala的集合排序是在内存中进行的 但是内存有可能不够用
         //可以调用RDD的sortby方法 内存+磁盘进行排序 可以把中间结果写磁盘 而且可以在多台机器并行



        //触发了多个action  内存不会溢出
        for(e<- subjects) {

            //该RDD中对应的数据仅有一个学科的数据 （因为过滤了）
            val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == e)

            //现在调用的是RDD的sortby方法 (take是一个action 会触发任务提交到集群)
            val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2, false).take(N)

            println(favTeacher.toBuffer)
        }
        sc.stop()
    }
}
