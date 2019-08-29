package day3

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  *
    数据文件
  * http://bigdata.edu360.cn/laozhang
    http://bigdata.edu360.cn/laozhang
  * 求每个学科中最受欢迎老师的top3（至少用2到三种方式实现）
  *
  */
object GroupFavTeacher1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
        val sc = new SparkContext(conf)

        val lines: RDD[String] = sc.textFile(args(0))
        //整理数据  学科 老师
        val subjectAndteacher:RDD[((String,String),Int)] = lines.map(line => {
            val index = line.lastIndexOf("/")
            val teacher = line.substring(index + 1)
            val httpHost = line.substring(0,index)
            val subject = new URL(httpHost).getHost.split("[.]")(0)
            ((subject, teacher),1)
        })

        //和 1 组合在一起 不好 调用了两次map方法
        //val map: Any => RDD[Nothing] = subjectAndteacher.map(_,1)

        //聚合 将学科和老师联合当做key 按照key对value进行聚合
        val reduced: RDD[((String, String), Int)] = subjectAndteacher.reduceByKey(_+_)

        //局部排序
        //分组排序(按学科分组) 把key 相同的数据搞到同一个分区里 从来自不同的机器所以会产生shuffle
        //(_._1._1) [学科,该学科对应的数据]
        val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

        //经过分组之后 一个分区内可能有多个学科的数据 一个学科就是一个迭代器
        //将每一个组拿出来进行操作
        //为什么可以调用scala的sortby方法 因为一个学科的数据已经在一台机器上的一个scala集合中  调用scala中的sortby 是为了局部排序
        val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))

        //收集结果
        val r: Array[(String, List[((String, String), Int)])] = sorted.collect()

        //打印
        println(r.toBuffer)
        sc.stop()
    }
}
