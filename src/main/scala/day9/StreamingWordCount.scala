package day9

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {

    def main(args: Array[String]): Unit = {

        //离线任务是创建sparkcontext，现在要实现实时计算 用streamingcontext
        val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")

        val sc = new SparkContext(conf)
        //StreamingContext 是对SparkContext的包装,包了一层就增加了实时的功能
        //第二个参数：小批次产生的时间间隔
        val ssc = new StreamingContext(sc,Milliseconds(5000))
        //有了StreamingContext就可以创建sparkStreaming的抽象了DSteam
        //从一个socket端口中读取数据
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node-11",8888)
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
