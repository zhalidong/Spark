package day7

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zx on 2017/9/18.
  */
object CsvDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("CsvDataSource")
      .master("local[*]")
      .getOrCreate()

    //指定以后读取json类型的数据
    val csv: DataFrame = spark.read.csv("d://csv")

    csv.printSchema()
    //给csv文件中字段起名字 字段类型都是string
    val pdf: DataFrame = csv.toDF("id", "name", "age")

    pdf.show()

    spark.stop()


  }
}
