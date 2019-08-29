package cn.edu360.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestScalaWordCount {

    def main(args: Array[String]): Unit = {
        //创建spark配置，设置应用程序名字
        val cof: SparkConf = new SparkConf().setAppName("TestScalaWordCount").setMaster("local[4]")

        val sc: SparkContext = new SparkContext(cof)
        val lines: RDD[String] = sc.textFile(args(0))
        //切分
        val words = lines.flatMap(s=>{s.split(" ")})

        val wordandone = words.map(s=>(s,1))

        val reduced = wordandone.reduceByKey((a,b)=>{a+b})

        val sorted = reduced.sortBy((a)=>{a._2})

        sorted.saveAsTextFile(args(1))
        sc.stop()
    }

}
