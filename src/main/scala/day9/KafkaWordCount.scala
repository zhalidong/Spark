package day9

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
        val ssc = new StreamingContext(conf,Seconds(5))
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
        //聚合
        val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
        //打印结果(Action)
        reduced.print()

        //启动sparkStreaming程序
        ssc.start()
        //等待优雅的退出
        ssc.awaitTermination()

    }

}
