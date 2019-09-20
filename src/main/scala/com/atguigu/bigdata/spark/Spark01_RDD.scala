package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zld on 2019/9/20 0020.
  */
object Spark01_RDD {

  def main(args: Array[String]): Unit = {
	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD")
	val sc = new SparkContext(config)
	//创建RDD
	//1.从内存中创建makeRDD  自定义分区为2 底层实现就是 parallelize
	val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
	//2.内存中创建
	val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4))
	//3.从外部存储中创建 可以读取项目路径 默认从文件中读取的数据都是字符串类型
	//读取文件时 传递的分区参数是最小分区数 但是不一定是这个分区数 取决于hadoop读取文件时分片规则
//	val fileRDD: RDD[String] = sc.textFile("in")

//	listRDD.collect().foreach(println)


	//将RDD中的数据保存到文件中
	listRDD.saveAsTextFile("output")
  }

}
