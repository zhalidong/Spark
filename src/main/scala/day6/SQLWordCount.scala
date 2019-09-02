package day6

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQLWordCount {

    def main(args: Array[String]): Unit = {

        //创建sparkSession
        val spark= SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate()

        //(指定以后从哪里)读取数据 是lazy
        //Dataset 也是一个分布式数据集,是对RDD的进一步封装，是更加智能的RDD
        //DataSet只有一列  默认这列叫value
        val lines: Dataset[String] = spark.read.textFile("hdfs://node-10:9000/words")
        //整理数据(切分压平)
        //导入隐式转换
        import  spark.implicits._
        val words: Dataset[String] = lines.flatMap(_.split(" "))
        //注册视图
        words.createTempView("v_wc")

        //执行sql（Transformation lazy）
        val result: DataFrame = spark.sql("select value, count(1) as counts from v_wc group by value order by counts desc")
        //执行Action
        result.show()

        spark.stop()

    }

}
