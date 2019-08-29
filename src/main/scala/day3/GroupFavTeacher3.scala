package day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  *2.求每个学科中最受欢迎老师的top3（至少用2到三种方式实现）
  *    http://bigdata.edu360.cn/laozhao
  *    相同学科的数据shuffle到同一个分区 自定义分区器
  *
  */
object GroupFavTeacher3 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("GroupFavTeacher3").setMaster("local[4]")
        val sc = new SparkContext(conf)


        val lines: RDD[String] = sc.textFile(args(0))
        //整理数据
        val subjectAndteacher:RDD[((String,String),Int)] = lines.map(line => {
            val index = line.lastIndexOf("/")
            val teacher = line.substring(index + 1)
            val httpHost = line.substring(0,index)
            val subject = new URL(httpHost).getHost.split("[.]")(0)
            ((subject, teacher),1)
        })

        //聚合 将学科和老师联合当做key
        val reduced: RDD[((String, String), Int)] = subjectAndteacher.reduceByKey(_+_)

        //计算有多少学科 拿出学科
        val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
        //自定义分区器 并且按照指定的分区器进行分区
        val sbparitioner: SubjectParitioner = new SubjectParitioner(subjects)


        //partitionBy按照指定的分区规则进行分区
        //调用partitionBy时候RDD的key是(String, String) 学科 老师
        val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbparitioner)

        //如何一次拿出一个分区 (可以操作一个分区中的数据了)
        val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
            //将迭代器转换成list排序再转换成迭代器返回
            it.toList.sortBy(_._2).reverse.take(3).iterator
        })
        val r: Array[((String, String), Int)] = sorted.collect()
        println(r.toBuffer)
        sc.stop()
    }
}

//自定义分区器 规则传过来学科返回分区编号
class SubjectParitioner(sbs:Array[String]) extends Partitioner{
    //相当于主构造器 (new的时候会执行一次)
    //用于存放规则的一个map
    val rule = new mutable.HashMap[String,Int]()
    var  i =0
    for(sb <-sbs){
//         rule(sb)=i
        rule.put(sb,i)
        i+=1
    }


    //返回分区的数量 （下一个RDD有多少分区）
    override def numPartitions = sbs.length

    //根据传入的key计算分区编号  key是一个元祖 (String,String)
    override def getPartition(key: Any) = {
        //获取学科名称  asInstanceOf强转成元祖
        val ss: String = key.asInstanceOf[(String,String)]._1

        //根据规则计算分区编号
        rule(ss)    //rule.get()
    }
}