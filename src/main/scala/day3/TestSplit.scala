package day3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestSplit {

    def main(args: Array[String]): Unit = {
        /*val line = "http://bigdata.edu360.cn/laozhang"

        val splits: Array[String] = line.split("/")
        val subject=splits(2).split("[.]")
        val subjects = subject(0)
        val teacher=splits(3)

        println(subjects+":"+teacher)*/


        val ss: String => (String, Int) = (line: String) => {
            val index = line.lastIndexOf("/")
            val teacher = line.substring(index + 1)
            //            val httpHost = line.substring(0,index)
            //            val subject = new URL(httpHost).getHost.split("[.]")(0)
            (teacher, 1)
        }




    }

}
