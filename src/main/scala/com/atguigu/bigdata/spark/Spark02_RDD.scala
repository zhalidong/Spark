package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zld on 2019/9/20 0020.
  */
object Spark02_RDD {

  def main(args: Array[String]): Unit = {
	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_RDD")
	val sc = new SparkContext(config)

	//map算子
	val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
	//mapPartitions可以对一个RDD中所有的分区进行遍历 分区中的数据是使用的scala的map

	//mapPartitions效率优于map算子 减少了发送到执行器执行交互次数
	//mapPartitions可能出现内存溢出
	val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas=>{
	  datas.map(_*2)		//scala中的map
	})


	mapPartitionsRDD.collect().foreach(print)


  }

}
