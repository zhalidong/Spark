package project_course

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用spark streaming处理kafka过来的数据
  */
object kafka_streaming {

    def main(args: Array[String]): Unit = {

        /*if(args.length!=4){
            println("Usage: zkQuorum,groupId,topciMap,numThreads")
            System.exit(1)
        }*/

        //进来的参数让如数组中
        //数据从哪里读 指定zk地址
        val zkQuorum = "node-10:2181,node-11:2181,node-12:2181"
        val groupId = "zld2"
        val topciMap = Map[String, Int]("first" -> 1)



        val sparkConf: SparkConf = new SparkConf().setAppName("kafka_streaming").setMaster("local[4]")
        val ssc = new StreamingContext(sparkConf,Seconds(60))

        val messages: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topciMap)
        //测试条数
        //messages.map(_._2).count().print()

        //数据清洗 拿到URL  "GET /class/112.html HTTP/1.1"
        val logs: DStream[String] = messages.map(_._2)
        val cleanData= logs.map(line => {
            val infos: Array[String] = line.split("\t")
            //infos(2) = "GET /class/112.html HTTP/1.1"
            //url = /class/112.html
            val url: String = infos(2).split(" ")(1)
            var courseId = 0

            if (url.startsWith("/class")){
                val courseIdHTML: String = url.split("/")(2)
                //112 课程编号
                courseId = courseIdHTML.substring(0,courseIdHTML.lastIndexOf(".")).toInt
            }
            //把数据装入一个ClickLog对象
            ClickLog(infos(0),DataUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))


        }).filter(clicklog=>clicklog.courseId!=0)
        //cleanData.print()

        //步骤三 统计今天到现在为止实战课程的访问量
        cleanData.map(x=>{

            //hbase rowkey设计: 20171111_88
            (x.time.substring(0,8)+"_"+x.courseId,1)
            //把DStream写入到hbase中通过调用foreachRDD 和 foreachPartition
        }).reduceByKey(_ + _).foreachRDD(rdd=>{
            rdd.foreachPartition(partitionRecords =>{
                val list = new ListBuffer[CourseClickCount]
                partitionRecords.foreach(pair=>{
                    list.append(CourseClickCount(pair._1,pair._2))
                })
                CourseDao.save(list)
            })
        })


        ssc.start()
        ssc.awaitTermination()

    }

}
