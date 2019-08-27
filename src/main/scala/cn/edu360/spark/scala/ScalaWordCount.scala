package cn.edu360.spark.scala


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {

  def main(args: Array[String]): Unit = {
      //创建spark配置 设置应用程序名字
      //val conf = new SparkConf().setAppName("ScalaWordCount")
      val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[4]")

      //创建spark执行的入口
      val sc = new SparkContext(conf)

      //指定以后从哪里了读取数据创建RDD(弹性分布式数据集)  类似集合
      val lines:RDD[String] = sc.textFile(args(0))
      //切分压平
      val words: RDD[String] = lines.flatMap(_.split(" "))


      //将单词和1组合
      val wordAndOne: RDD[(String, Int)] = words.map((_,1))

      //按key进行聚合
      val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

      //排序
      val sorted: RDD[(String, Int)] = reduced.sortBy(_._2,false)

      //将结果保存到hdfs中
      sorted.saveAsTextFile(args(1))

      //该函数的功能是将对应分区中的数据取出来，并且带上分区编号
      /*val func=(index:Int,it:Iterator[Int])=>{
          it.map(x=>s"part: $index, ele: $x")
      }*/

//      lines.mapPartitionsWithIndex()



      //释放资源
      sc.stop()

  }

}
