package Bike_indicators

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.json.JSONObject

object Bike_kafka_streaming02 {
    def main(args: Array[String]): Unit = {

        val group = "zld111"
        val conf = new SparkConf().setAppName("Bike_kafka_streaming02").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Duration(5000))
        val topic = "recharge"
        val brokerList = "node-10:9092,node-11:9092,node-12:9092"
        val zkQuorum = "node-10:2181,node-11:2181,node-12:2181"
        val topics: Set[String] = Set(topic)
        val topicDirs = new ZKGroupTopicDirs(group, topic)
        val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "node-10,node-11,node-12")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

        val tableName = "city_recharge_amount"
        val jobConf = new JobConf(hbaseConf)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)


        val kafkaParams = Map(
            "metadata.broker.list" -> brokerList,
            "group.id" -> group,
            "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
        )

        val zkClient = new ZkClient(zkQuorum)
        val children = zkClient.countChildren(zkTopicPath)
        var kafkaStream: InputDStream[(String, String)] = null
        var fromOffsets: Map[TopicAndPartition, Long] = Map()

        if (children > 0) {
            for (i <- 0 until children) {
                val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
                val tp = TopicAndPartition(topic, i)
                fromOffsets += (tp -> partitionOffset.toLong)
            }

            val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

            kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
        } else {
            kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        }

        var offsetRanges = Array[OffsetRange]()

        kafkaStream.foreachRDD{ kafkaRDD =>
            if(!kafkaRDD.isEmpty()) {
                offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
                val lines: RDD[String] = kafkaRDD.map(_._2)

                val value: RDD[dataMode2] = kafkaRDD.map(x => {
                    val a = x._2.toString
                    val obj: JSONObject = new JSONObject(a)
                    val phoneNum = obj.getString("phoneNum")
                    val amount = obj.getInt("amount")
                    val date = obj.getString("date")
                    dataMode2(phoneNum, amount,date)
                })

                value.map(x =>{
                    val put = new Put(Bytes.toBytes(x.phoneNum))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("amount"), Bytes.toBytes(x.amount))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("date"), Bytes.toBytes(x.date))
                    (new ImmutableBytesWritable, put)
                }).saveAsHadoopDataset(jobConf)

                for (o <- offsetRanges) {
                    val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
                    ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
                }
            }
        }
        ssc.start()
        ssc.awaitTermination()
    }
}

// 定义case类来析构json数据
//case class dataModel (keyno:String,name:String,oper:String,Partners:String)
case class dataMode2 (phoneNum:String,amount:Int,date:String)

