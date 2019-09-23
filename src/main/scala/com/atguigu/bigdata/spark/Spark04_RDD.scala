package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zld on 2019/9/20 0020.
  */
object Spark04_RDD {

  def main(args: Array[String]): Unit = {
	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_RDD")
	val sc = new SparkContext(config)

	//mapPartitionsWithIndex算子
	val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))

	//flatMap
	val result: RDD[Int] = listRDD.flatMap(datas=>datas)



  }

}
