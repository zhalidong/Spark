package day7

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zx on 2017/9/18.
  */
object JsonDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JsonDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //指定以后读取json类型的数据(有表头)
    //val jsons: DataFrame = spark.read.json("d://json")
    val jsons: DataFrame = spark.read.json("hdfs://node-10:9000/bike/recharge/20190913/recharge-.1568386139841")

    //val filtered: DataFrame = jsons.where($"age" <=500)


    jsons.printSchema()

    jsons.show()

    spark.stop()


  }
}
