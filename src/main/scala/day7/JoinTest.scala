package day7

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinTest {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()
        import spark.implicits._

        val lines: Dataset[String] = spark.createDataset(List("1,laozhao,china","2,laoduan,usa","3,laoyang,jp"))
        //数据整理
        val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
            val fields: Array[String] = line.split(",")
            val id = fields(0).toLong
            val name = fields(1)
            val nationCode = fields(2)

            (id, name, nationCode)
        })
        val df1: DataFrame = tpDs.toDF("id","name","nation")

        val nations: Dataset[String] = spark.createDataset(List("china,中国","usa,美国"))
        val ndataset: Dataset[(String, String)] = nations.map(l => {
            val fields: Array[String] = l.split(",")
            val ename = fields(0)
            val cname = fields(1)
            (ename, cname)
        })
        val df2: DataFrame = ndataset.toDF("ename","cname")

        //第一种 创建视图
        /*df1.createTempView("v_users")
        df2.createTempView("v_nations")

        val result: DataFrame = spark.sql("select name,cname from v_users join v_nations on nation=ename")
        result.show()*/

        val r: DataFrame = df1.join(df2,$"nation"===$"ename","left")
        r.show()
        spark.stop()

    }

}
