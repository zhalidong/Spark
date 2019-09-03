package day7
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
object JdbcDataSource {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder().appName("JdbcDataSource")
                .master("local[*]")
                .getOrCreate()

        import spark.implicits._

        //load这个方法会读取真正mysql的数据吗？
        val logs: DataFrame = spark.read.format("jdbc").options(
            Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
                "driver" -> "com.mysql.jdbc.Driver",
                "dbtable" -> "logs",
                "user" -> "root",
                "password" -> "smallming")
        ).load()

        //logs.printSchema()


        //logs.show()

//            val filtered: Dataset[Row] = logs.filter(r => {
//              r.getAs[Int]("age") <= 13
//            })
//            filtered.show()

        //lambda表达式
        val r = logs.filter($"age" <= 13)
//        r.show()
        //val r = logs.where($"age" <= 13)

        val reslut: DataFrame = r.select($"id", $"name", $"age" * 10 as "age")
        //reslut.show()
        //val props = new Properties()
        //props.put("user","root")
        //props.put("password","smallming")
        //reslut.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata", "logs1", props)

//        DataFrame保存成text时出错(只能保存一列)并且是string类型
        //reslut.write.text("d://text")

        //reslut.write.json("d://json")

        //reslut.write.csv("d://csv")


        //reslut.write.parquet("hdfs://node-10:9000/parquet")


        //reslut.show()

        spark.close()


    }
}
