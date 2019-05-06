package utils
import org.apache.kudu.Type
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object SparkKuduUtil {
  //获取kuduMaster
  val kuduMaster: String = PropertiesUtils.loadProperties("kuduMaster")

  def saveRddToKudu(sc: SparkContext,df: DataFrame,tableName:String,primaryKeys:List[String]) = {
    this.synchronized {
      val kuduContext = new KuduContext(kuduMaster, sc)
      val sqlContext = new SQLContext(sc)
      //1.先判断表是否存在,不存在创建
      if (!kuduContext.tableExists(tableName)) {

        //通过impala JDBC 的方式创建kudu表
        //      val connection = connectImpala()
        //
        //      val stmt: Statement = connection.createStatement()
        //      val sql = "\tCREATE TABLE default.events (   time STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,   PRIMARY KEY (time) ) PARTITION BY HASH (time) PARTITIONS 4 STORED AS KUDU TBLPROPERTIES ('kudu.master_addresses'='node51:7051')"
        //      stmt.executeUpdate(sql)
        //
        //      close(connection,stmt)
        //通过下面方法可以再kudu中声称表,但是不能在impala中查到
        val schema: StructType = df.schema
        val kuduTableOptions = new CreateTableOptions()
        kuduTableOptions.
          setRangePartitionColumns(primaryKeys.asJava).
          setNumReplicas(3)
        kuduContext.createTable(tableName,schema,primaryKeys,kuduTableOptions)
        println("成功创建表 " + tableName)
      }

      //2.若表存在,判断dataframe schema与kudu表 schema是否相同,若有新字段,添加kudu表字段
      val structArray: ArrayBuffer[StructField] = ArrayBuffer[StructField]()//存放新增的字段
      val kuduOptions: Map[String, String] = Map(
        "kudu.table" -> tableName,
        "kudu.master" -> kuduMaster)
      val kuduScheme: StructType = sqlContext.read.options(kuduOptions).kudu.schema
      println(kuduScheme.size + " kudu表 " + tableName + " 的字段个数")
//      sqlContext.read.options(kuduOptions).kudu.show(5)

      val dfSchema: StructType = df.schema
      for (struct <- dfSchema.fields) {
        if (!kuduScheme.contains(struct)) {
          structArray.append(struct)
        }
      }

      if (!structArray.isEmpty) {//kudu新增字段
        //      var connection: Connection = null
        //      var stmt: Statement = null
        for (struct <- structArray) {
          val column: String = struct.name
          println("新增字段 " + column)
          //这种添加可以生效,但是在impala中需要等待一段时间才可以显示
          kuduContext.syncClient.alterTable(tableName,new AlterTableOptions().addNullableColumn(column, Type.STRING))
          //使用impala添加更加实时,但是带符号的字段添加不了
          //         connection = connectImpala()
          //         stmt = connection.createStatement()
          //        val sql = s"ALTER TABLE events ADD COLUMNS  (`$column` string)"
          //        println(sql)
          //        stmt.executeUpdate(sql)
//          sqlContext.read.options(kuduOptions).kudu.show()
        }
        //      close(connection,stmt)
      }

      //3.将dataframe存入kudu中
      try {
        if (!df.rdd.isEmpty()) {
          var finalKuduSchema: StructType = new StructType()
          for (struct <- dfSchema.fields) {
            finalKuduSchema = sqlContext.read.options(kuduOptions).kudu.schema
            while (!finalKuduSchema.contains(struct)) {
              println("新增字段还未更新")
              Thread.sleep(100)
              finalKuduSchema = sqlContext.read.options(kuduOptions).kudu.schema
            }
          }
          kuduContext.upsertRows(df, tableName)
        }
      } catch {
        case ex: Exception => "dataframe写入kudu失败,错误信息为： " + ex.getMessage
      }
//      sqlContext.read.options(kuduOptions).kudu.show()
    }

  }

  def readFromKudu(sc: SparkContext,tableName:String) ={
    val sqlContext = new SQLContext(sc)
    val kuduOptions: Map[String, String] = Map(
      "kudu.table" -> tableName,
      "kudu.master" -> kuduMaster)
    val users: DataFrame = sqlContext.read.options(kuduOptions).kudu
    users
  }


}
