package day6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 非结构化数据变成结构化数据
  */
object SQLDemo2 {

    def main(args: Array[String]): Unit = {
        //提交的这个程序可以连接到spark集群中
        val conf = new SparkConf().setAppName("SQLDemo2").setMaster("local[*]")

        //创建sparksql的连接（程序执行的入口）
        val sc = new SparkContext(conf)
        //SparkContext不能创建特殊的RDD(DataFrame)
        //将SparkContext包装 进而增强
        val sqlContext = new SQLContext(sc)
        //创建特殊的RDD（DataFrame）,就是有schema信息的RDD

        //先有一个普通的RDD 然后在关联上schema 进而转换成DataFrame
        val lines=sc.textFile("hdfs://node-10:9000/person")
        //将数据进行整理
        val rowRDD: RDD[Row] = lines.map(line => {
            val fields = line.split(",")
            val id = fields(0).toLong
            val name = fields(1)
            val age = fields(2).toInt
            val fv = fields(3).toDouble
            Row(id,name,age,fv)
        })
        //结构类型 其实就是表头，用于描述DataFrame
        val sch = StructType(List(
            StructField("id", LongType, true),
            StructField("name", StringType, true),
            StructField("age", IntegerType, true),
            StructField("fv", DoubleType, true)
        ))
        //将ROWRDD关联schema
        val bdf: DataFrame = sqlContext.createDataFrame(rowRDD,sch)
        //将DataFrame注册成临时表
        bdf.registerTempTable("t_boy")

        //书写sql(SQL方法是Transformation)
        val result: DataFrame = sqlContext.sql("select * from t_boy order by fv desc,age asc")
        
        //查看结果(触发Action)
        result.show()


        //释放资源
        sc.stop()
    }

}
