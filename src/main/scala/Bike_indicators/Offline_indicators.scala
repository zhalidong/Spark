package Bike_indicators

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**共享单车指标计算
  * sparlsql从HDFS中拉取数据进行计算(json数据)
  * 离线指标:
  *
  *
  */
object Offline_indicators {

  def main(args: Array[String]): Unit = {

    /*val spark = SparkSession.builder().appName("JsonDataSource")
      .master("local[*]")
      .getOrCreate()*/


    val conf = new SparkConf()
    conf.setMaster("local").setAppName("jsonfile")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    //指定以后读取json类型的数据(有表头)
    //val jsons: DataFrame = sqlContext.read.json("hdfs://node-10:9000/bike/recharge/20190913/recharge-.1568386139841")
    val jsons: DataFrame = sqlContext.read.json("hdfs://node-10:9000/bike/recharge/20190913")





    jsons.registerTempTable("log")
      //充值总金额
    val result: DataFrame = sqlContext.sql("select  sum(amount) from log")


    result.show()
    /*result.foreachPartition(it=>{
        val config = HbaseConfiguratuion.create
        config.set("hbase的对接zookeeper端口号":"2181")
        config.set("hbase对接的zookepper的集群的位置","a1,a2,a3")
        val connetion=ConnectionFactory.createConnection(config)
        val table=connection.getTable(TableName.valueoF("res:user_rec"))
        val list = new java.util.ArrayList[Put]
        table.put(list)
        //分区数据写入到hbase之后关闭
        table.close
    })*/
      //hbase自带的api 使用put的方法


    sc.stop()


  }
}
