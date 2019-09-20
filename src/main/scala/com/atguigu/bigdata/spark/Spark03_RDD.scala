package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zld on 2019/9/20 0020.
  */
object Spark03_RDD {

  def main(args: Array[String]): Unit = {
	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_RDD")
	val sc = new SparkContext(config)

	//mapPartitionsWithIndex算子
	val listRDD: RDD[Int] = sc.makeRDD(1 to 10,2)

	val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
	  case (num, datas) => {
		datas.map((_, "分区号" + num))
	  }
	}


	tupleRDD.collect().foreach(println)


  }

}
