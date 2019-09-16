package Hbase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes



object HbaseUtils {
    // 创建hbase配置文件
    val con: Configuration = HBaseConfiguration.create()
    // 设置zookeeperjiqun
    con.set("hbase.zookeeper.quorum","node-10:2181,node-11:2181,node-12:2181")
    // 建立连接
    val conf: Connection = ConnectionFactory.createConnection(con)
    // 获取管理员
    val admin: Admin = conf.getAdmin
    // 创建表名称对象，通过这个对象才可以操作表
    val tableName: TableName = TableName.valueOf("student")
    // 通过连接对象获取表，操作表中数据
    val table: Table = conf.getTable(tableName)


    def main(args: Array[String]): Unit = {
        create(List("info","data"))
    }

    //todo:创建一个hbase表    一个属性，用于规定列族
    //create 'student','info'
    def create(columnFamily:List[String]): Unit ={
        //创建 hbase 表描述
        val tname = new HTableDescriptor(tableName)
        //添加列族，新建行模式当做列族
        columnFamily.foreach(x=>tname.addFamily(new HColumnDescriptor(x)))
        admin.createTable(tname)
        println("创建表成功")
    }

}
