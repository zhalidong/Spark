package Bike_indicators

import HBaseUtils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object BikeDao {
    val tableName = "recharge"
    val cf = "info"
    val qualifer = "phoneNum"

    def save(list:ListBuffer[BikePojo])= {
        val table: HTable = HBaseUtils.getInstance().getTable(tableName)

        for (ele <- list) {
            table.incrementColumnValue(Bytes.toBytes(ele.phoneNum),
                Bytes.toBytes(cf),
                Bytes.toBytes(qualifer),
                ele.amount)
        }

    }
        def count(day_course:String):Long={
            val table: HTable = HBaseUtils.getInstance().getTable(tableName)
            val get = new Get(Bytes.toBytes(day_course))
            val value: Array[Byte] = table.get(get).getValue(cf.getBytes,qualifer.getBytes)

            if(null==value){
                0L
            }else{
                Bytes.toLong(value)
            }


        }

        def main(args: Array[String]): Unit = {
            val list = new ListBuffer[BikePojo]
            list.append(BikePojo("13585600404",45,"2019-09-13T04:36:20.435Z",31.296217,121.458492,"上海市","上海市","静安区"))
            save(list)

        }





}
