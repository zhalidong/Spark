package day9

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 累加历史批次数据  容易丢（当重启程序后）
  * updateStateByKey此函数会保存以前批次的数据进行聚合
  */
object StatefulKafkaWordCount {


    /**
      * 第一个参数：聚合的key，就是单词
      * 第二个参数：当前批次产生 该单词在每一个分区出现的次数
      * 第三个参数：初始值或累加的中间结果
      */
    val updateFunc=(iter: Iterator[(String,Seq[Int],Option[Int])])=>{
        //iter.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))
        //scala模式匹配 y.sum 当前批次数据 z.getOrElse(0)历史数据
        iter.map{ case (x,y,z)=>(x,y.sum+z.getOrElse(0))}

    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
        val ssc = new StreamingContext(conf,Seconds(5))

        //如果要使用可更新历史数据（累加历史数据）那么就要把中间结果保存起来
        ssc.checkpoint("d://ck")


        //数据从哪里读 指定zk地址
        val zkQuorum = "node-10:2181,node-11:2181,node-12:2181"
        val groupId = "g1"
        val topic = Map[String, Int]("xiaoniuabc" -> 1)

        //创建DStream 需要kafkaDStream
        val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topic)
        //对数据处理
        //kafka的ReceiverInputDStream[(String, String)] 里面装的是一个元祖 (key是写入的key,value是实际写入的内容)
        val lines: DStream[String] = data.map(_._2)

        //对DSteam进行操作,你操作这个抽象（代理），就像操作一个本地集合一样
        //切分压平
        val words: DStream[String] = lines.flatMap(_.split(" "))
        //单词和1组合到一起
        val wordAndOne: DStream[(String, Int)] = words.map((_,1))
        //聚合 updateStateByKey此函数会保存以前批次的数据进行聚合 HashPartitioner分区器
        val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
        //打印结果(Action)
        reduced.print()

        //启动sparkStreaming程序
        ssc.start()
        //等待优雅的退出
        ssc.awaitTermination()

    }
}
