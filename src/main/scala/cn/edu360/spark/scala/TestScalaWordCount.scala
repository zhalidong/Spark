package cn.edu360.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestScalaWordCount {

    def main(args: Array[String]): Unit = {
        //创建spark配置，设置应用程序名字
        val cof: SparkConf = new SparkConf().setAppName("TestScalaWordCount").setMaster("local[4]")

        val sc: SparkContext = new SparkContext(cof)
        val lines: RDD[String] = sc.textFile(args(0))
        //切分压平
        val words: RDD[String] = lines.flatMap(_.split(" "))
        //将单词和1组合
        val wordAndOne: RDD[(String, Int)] = words.map((_,1))
        //按key聚合
        val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)


    }

}
