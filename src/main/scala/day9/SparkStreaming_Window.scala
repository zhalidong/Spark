package day9

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_Window {

    def main(args: Array[String]): Unit = {
        //scala中的窗口
        val ints = List(1,2,3,4,5,6)
        //滑动窗口函数
        val intses: Iterator[List[Int]] = ints.sliding(3,3)
        for (elem <- intses) {
            println(elem.mkString(","))
        }


        //sparkStreaming的窗口
        val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_Window")
        val streamingcontext = new StreamingContext(sparkconf,Seconds(3))

        //窗口大小应该是采集周期的整数倍   窗口滑动的步长也应该是采集周期的整数倍  3的倍数
        //window(Seconds(9),seconds(3))


        streamingcontext.start()
        streamingcontext.awaitTermination()

    }

}
