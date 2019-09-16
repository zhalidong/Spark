/*
package Bike_indicators

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject
import org.json4s.DefaultFormats

/**
  * 使用sparkstreaming处理kafka过来的数据
  * {"phoneNum":"13585608888","amount":100,"date":"2019-09-13T06:57:50.211Z","lat":31.296217,"log":121.458492,"province":"上海市","city":"上海市","district":"静安区"}
  *
  *
  */
object Bike_kafka_Streaming {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Bike_kafka_Streaming").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf,Seconds(60))

        /*val zkQuorum = "node-10:2181,node-11:2181,node-12:2181"
        val groupId = "g12"
        val topicMap = Map[String, Int]("recharge" -> 1)*/
        //hbase
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "node-10:2181,node-11:2181,node-12:2181")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        val tableName = "circle"




        val topic = "recharge"
        val brokerList = "node-10:9092,node-11:9092,node-12:9092"
        val group = "bike1"
        //准备kafka的参数
        val kafkaParams = Map(
            "metadata.broker.list" -> brokerList,
            "group.id" -> group,
            //从头开始读取数据
            "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
        )
        val topicDirs = new ZKGroupTopicDirs(group, topic)
        val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
        val zkQuorum = "node-10:2181,node-11:2181,node-12:2181"
        val zkClient = new ZkClient(zkQuorum)
        val children = zkClient.countChildren(zkTopicPath)
        var fromOffsets: Map[TopicAndPartition, Long] = Map()
        val topics: Set[String] = Set(topic)
        var kafkaStream: InputDStream[(String, String)] = null
        if (children > 0) {
            for (i <- 0 until children) {

                val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
                val tp = TopicAndPartition(topic, i)
                fromOffsets += (tp -> partitionOffset.toLong)
            }
            val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

            kafkaStream=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
        } else {
            kafkaStream=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        }
        var offsetRanges = Array[OffsetRange]()
        val transform: DStream[(String, String)] = kafkaStream.transform { rdd =>
            //得到该 rdd 对应 kafka 的消息的 offset
            //该RDD是一个KafkaRDD，可以获得偏移量的范围
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }
        //val messages: DStream[String] = transform.map(_._2)

        transform.foreachRDD(kafkaRDD =>
            if(!kafkaRDD.isEmpty()) {
                offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
                val lines: RDD[String] = kafkaRDD.map(_._2)
                val value: RDD[dataModel] = kafkaRDD.map(x => {
                    val a = x._2.toString
                    val obj: JSONObject = new JSONObject(a)
                    val phoneNum = obj.getString("phoneNum")
                    val amount = obj.getInt("amount")
                    val date = obj.getString("date")

                    dataModel(phoneNum, amount,date)
                })



                /*value.map(x =>{
                    val put = new Put(Bytes.toBytes(x.phoneNum))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("companyname"), Bytes.toBytes(x.amount))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("oper"), Bytes.toBytes(x.date))
                    (new ImmutableBytesWritable, put)
                }).saveAsHadoopDataset(jobConf)*/

        })





        //val messages: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topicMap)
        ssc.start()
        ssc.awaitTermination()
    }

}
// 定义case类来析构json数据
//case class dataModel (phoneNum:String,amount:Int,date:String)*/
