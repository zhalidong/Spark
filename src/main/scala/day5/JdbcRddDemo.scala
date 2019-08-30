package day5

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRddDemo {

    def main(args: Array[String]): Unit = {

        val getConn=()=>{
            DriverManager.getConnection("jdbc:mysql://localhost:3306/ssm?characterEncoding=UTF-8", "root", "smallming")
        }

        val conf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[*]")
        val sc = new SparkContext(conf)

        //创建RDD 这个RDD会记录以后从Mysql中读数据
        //new 了RDD 里面没有真正要计算的数据 而是告诉这个RDD  以后触发Action时 到哪里读取数据
        val jdbcRDD:RDD[(Int,String,Int)] = new JdbcRDD(
            sc,
            getConn,
            "select * from account where id>=? and id<= ?",
            1,
            4,
            2,                    //分区数量
            rs=>{
                val id=rs.getInt(1)
                val name=rs.getString(2)
                val balance = rs.getInt(3)
                (id,name,balance)
            }
        )

        //触发Action
        val r: Array[(Int, String, Int)] = jdbcRDD.collect()
        println(r.toBuffer)

        sc.stop()

    }

}
