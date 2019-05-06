import java.util
import net.sf.json.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object sparkStreamingToKuduOfSensorsData {

  def stopByMarkFile(ssc: StreamingContext): Unit = {
    val log = org.apache.log4j.LogManager.getLogger("SparkDirectStreaming")
    val intervalMills = 10 * 1000 // 每隔10秒扫描一次消息是否存在
    var isStop = false
    val hdfs_file_path: String = PropertiesUtils.loadProperties("streaming.stop.path") //判断消息文件是否存在，如果存在就停止spark程序
    while (!isStop) {
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExistsMarkFile(hdfs_file_path)) {
        log.warn("2秒后开始关闭sparstreaming程序.....")
        Thread.sleep(2000)
        ssc.stop(true, true)
      }
    }
  }

  def isExistsMarkFile(hdfs_file_path: String): Boolean = {
    val conf = new Configuration()
    val path = new Path(hdfs_file_path)
    val fs = path.getFileSystem(conf)
    fs.exists(path)
  }


  def main(args: Array[String]): Unit = {
    /*********************** 1.从kafka获取消息 ***********************/
    val sparkConf: SparkConf = new SparkConf().setAppName("sparkStreamingToKuduOfSensorsData")
    // 指定任务优雅地停止
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    //消费kafka的最大速率
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "30") //限制每秒每个消费线程读取每个kafka分区最大的数据量
    sparkConf.set("spark.streaming.backpressure.enabled", "true") //开启后spark自动根据系统负载选择最优消费速率
    sparkConf.set("spark.streaming.backpressure.initialRate", "50") //在backpressure.enabled开启时，限制第一次批处理应该消费的数据，防止第一次全部读取，造成系统阻塞，默认直接读取所有
    //创建streamingContext
    val interval = PropertiesUtils.loadProperties("streaming.interval").toLong
    val sparkSession: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()//写入hive enableHiveSupport()需开启
    val streamingContext: StreamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(interval))
//    sparkSession.sparkContext.setLogLevel("WARN")
    //kafka消费者创建
    val bootstrap_servers: String = PropertiesUtils.loadProperties("bootstrap.servers")
    val kafka_topic: String = PropertiesUtils.loadProperties("kafka.topic")
    val groupId: String = PropertiesUtils.loadProperties("group.id")
    val kafka_offset_table: String = PropertiesUtils.loadProperties("kafka_offset.table")
    //消费者配置
    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> bootstrap_servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //从Hbase中取出历史偏移量
    val fromOffsets: Map[TopicPartition, Long] = HbaseOffset.getLastCommittedOffsets(kafka_topic, groupId, kafka_offset_table)
    //创建Dstream
    val infoDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))
    val messageDstream: DStream[String] = infoDStream.transform(item => {
      item.map(x => x.value())
    })

//    messageDstream.cache()

    /*********************** 2.json消息并统计写入events表与users表中 ***********************/
    messageDstream.foreachRDD(rdd => {
      val messageCount: Long = rdd.dependencies(0).rdd.count()
      if (messageCount > 0) {
        println(messageCount + ">>>该批次消息数")

        //1-- 将json分类
        val eventsRdd = rdd.filter(x => x.contains("\"type\":\"track\""))
        val usersRdd = rdd.filter(x => {x.contains("\"type\":\"profile_")})
        val track_signupRdd = rdd.filter(x => {x.contains("\"type\":\"track_signup\"")})

        //2-- events表写入
        if (!eventsRdd.isEmpty()) {
          println(eventsRdd.getNumPartitions + " ==eventsRdd分区个数")
          val eventsJsonRdd = eventsRdd.map(x => {
            var type_string = ""
            var properties = ""
            var time = ""
            var event = ""
            var distinct_id = ""
            var user_id = ""
            val array: ArrayBuffer[StructField] = ArrayBuffer[StructField]()
            var keyValueMap: mutable.Map[String, String] = mutable.Map[String, String]() //存放properties里属性及值

            try {
              val json: JSONObject = JSONObject.fromObject(x)
              type_string = json.get("type").toString
              time = json.get("time").toString
              event = json.get("event").toString
              distinct_id = json.get("distinct_id").toString
              user_id = json.get("user_id").toString
              properties = json.get("properties").toString

              val propJson: JSONObject = JSONObject.fromObject(properties)
              val keys: util.Iterator[_] = propJson.keys()

              while (keys.hasNext) {
                val key: Any = keys.next()
                val value: AnyRef = propJson.get(key)
                array.append(StructField(key.toString, StringType, nullable = true))
                keyValueMap += (key.toString -> value.toString)
              }
            } catch {
              case ex: Exception => println("解析错误")
            }
            ((type_string, keyValueMap), (time, event, distinct_id, user_id, keyValueMap))
          })
          eventsJsonRddToHive(eventsJsonRdd, sparkSession)

        }

        //3-- users表写入
//        if (!usersRdd.isEmpty()) {
//          val userJsonRdd = usersRdd.map(x => {
//            var type_string = ""
//            var properties = ""
//            var time = ""
//            var user_id = ""
//            val array: ArrayBuffer[StructField] = ArrayBuffer[StructField]()
//            var keyValueMap: mutable.Map[String, String] = mutable.Map[String, String]() //存放properties里属性及值
//
//            try {
//              val json: JSONObject = JSONObject.fromObject(x)
//              type_string = json.get("type").toString
//              time = json.get("time").toString
//              user_id = json.get("user_id").toString
//              properties = json.get("properties").toString
//
//              val propJson: JSONObject = JSONObject.fromObject(properties)
//              val keys: util.Iterator[_] = propJson.keys()
//
//              while (keys.hasNext) {
//                val key: Any = keys.next()
//                val value: AnyRef = propJson.get(key)
//                array.append(StructField(key.toString, StringType, nullable = true))
//                keyValueMap += (key.toString -> value.toString)
//              }
//            } catch {
//              case ex: Exception => println("解析错误")
//            }
//            ((type_string, keyValueMap), (time, user_id, keyValueMap))
//          })
//          userJsonRdd.foreach(x =>{println(x)})
//          userJsonRddToKudu(userJsonRdd, sparkSession)
//        }
//
//        //4-- track_signup(events,users表)
//        if (!track_signupRdd.isEmpty()) {
//          val trackJsonRdd = track_signupRdd.map(x => {
//            var type_string = ""
//            var properties = ""
//            var time = ""
//            var event = ""
//            var user_id = ""
//            var distinct_id = ""
//            var original_id = ""
//            val array: ArrayBuffer[StructField] = ArrayBuffer[StructField]()
//            var keyValueMap: mutable.Map[String, String] = mutable.Map[String, String]() //存放properties里属性及值
//
//            try {
//              val json: JSONObject = JSONObject.fromObject(x)
//              type_string = json.get("type").toString
//              time = json.get("time").toString
//              event = json.get("event").toString
//              user_id = json.get("user_id").toString
//              distinct_id = json.get("distinct_id").toString
//              original_id = json.get("original_id").toString
//              properties = json.get("properties").toString
//
//              val propJson: JSONObject = JSONObject.fromObject(properties)
//              val keys: util.Iterator[_] = propJson.keys()
//
//              while (keys.hasNext) {
//                val key: Any = keys.next()
//                val value: AnyRef = propJson.get(key)
//                array.append(StructField(key.toString, StringType, nullable = true))
//                keyValueMap += (key.toString -> value.toString)
//              }
//            } catch {
//              case ex: Exception => println("解析错误")
//            }
//            ((type_string, keyValueMap), (time, event, distinct_id, user_id, original_id, keyValueMap))
//          })
//          trackJsonRdd.foreach(x =>{
//            println(x)})
//          trackJsonRddToKudu(trackJsonRdd, sparkSession)
//        }
      }
    })

    /*********************** 3.该批数据消费完后,将消费到的位置保存至Hbase ***********************/
    infoDStream.foreachRDD((rdd, batchTime) => {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      HbaseOffset.saveOffsets(kafka_topic, groupId, offsetRanges, kafka_offset_table, batchTime.toString)
    })

    streamingContext.start()
    stopByMarkFile(streamingContext)
    streamingContext.awaitTermination()
  }

  def eventsJsonRddToHive(eventsJsonRdd: RDD[((String, mutable.Map[String, String]), (String, String, String, String, mutable.Map[String, String]))], sparkSession: SparkSession) = {
    import sparkSession.sql
    val sqlContext = new SQLContext(sparkSession.sparkContext)
    println("======eventsJsonRddToHive======")

    val dfSchemaArray: ArrayBuffer[String] = ArrayBuffer[String]()//存放dataframe的schema
    val nonPropertiesList = List("time", "event", "distinct_id", "user_id","partition_date")//不在properties里的属性
    //dfSchemaArray中追加5个属性
    for (column <- nonPropertiesList) {dfSchemaArray.append(column)}

//    println(eventsJsonRdd.count() + ">>>eventsJsonRdd个数")

    val collectArr: Array[((String, mutable.Map[String, String]), (String, String, String, String, mutable.Map[String, String]))] = eventsJsonRdd.collect()//将Rdd所有数据放入collectArr中
    println(collectArr.size + "  collectArr数据条数")


    //创建包含该批次所有字段的dfSchemaArray
    for (message <- collectArr) {
      val properties: List[String] = message._1._2.keys.toList//properties里的属性keys
      //如果dfSchemaArray中没有该属性，则将该属性加入dfSchemaArray
      for (property <- properties if !dfSchemaArray.contains(property)) {dfSchemaArray.append(property)}
    }

    println(dfSchemaArray.size + "  dfSchemaArray里的字段个数")

    var finalHiveSchemaStructField = Array[StructField]()//最终dataframe的structField
    var finalHiveSchema: Array[String] = Array[String]()//最终dataframe个字段的集合

    //判断events表是否存在
    val tableExist: Boolean = sql("show tables in rawdata").collect().map(x => {x.get(1)}).contains("events_increment")

    if (tableExist) {
      //读取hive中events表字段，如果dfSchemaArray中有新增字段，通过alter table add columns添加
      val hiveSchema: Array[String] = sparkSession.table("rawdata.events_increment").schema.fields.map(_.name)
      var alterStatement = "alter table rawdata.events_increment add columns("

      for (dfSchema <- dfSchemaArray) {
        if (!hiveSchema.map(_.split("__")(0)).contains(dfSchema)) {//过滤出要添加的字段
          if (hiveSchema.map(_.toLowerCase().split("__")(0)).contains(dfSchema.toLowerCase())) {//处理大小写字段
            alterStatement += s"`${dfSchema + "__" + dfSchema.hashCode.toString.replaceAll("-","")}`" + " string,"
          }else{
            alterStatement += s"`$dfSchema`" + " string,"
          }

        }
      }

      val alterSql: String = alterStatement.substring(0,alterStatement.length()-1) + ")"

      if (alterSql.contains("string")) {//说明有新增字段
        println("sql字段命令==="+alterSql)
        println("有新增字段,执行alter语句")
        sql(s"$alterSql")//执行添加字段sql

        //判断新增字段是否添加成功
        finalHiveSchema = sparkSession.table("rawdata.events_increment").schema.fields.map(x =>x.name)
        try {
          for (struct <- dfSchemaArray) {
            while (!(finalHiveSchema.contains(struct) || finalHiveSchema.contains(struct + "__" + struct.hashCode.toString.replaceAll("-","")))) {
              println("新增字段还未更新")
              Thread.sleep(100)
              finalHiveSchema = sparkSession.table("rawdata.events_increment").schema.fields.map(x =>x.name)
            }
          }
        } catch {
          case ex: Exception => throw ex
        }
        finalHiveSchemaStructField = for (schema <- finalHiveSchema) yield StructField(schema, StringType, nullable = true)
      }else{//无新增字段
        println("无新增字段")
        finalHiveSchema = hiveSchema
        finalHiveSchemaStructField = for (schema <- finalHiveSchema) yield StructField(schema, StringType, nullable = true)
      }

    }else{
      println("events_increment表不存在")
      val distinct = dfSchemaArray.map(x =>(x.toLowerCase(),1)).groupBy(_._1).filter(x => x._2.size > 1).keys.toArray//小写后相同的字段 eg:(taskId && taskid)
      finalHiveSchema = dfSchemaArray.map(x => {
        if (distinct.contains(x.toLowerCase())) {
          x + "__" + x.hashCode.toString.replaceAll("-","")
        }else{
          x
        }
      }).toArray
      finalHiveSchemaStructField = for (schema <- finalHiveSchema) yield StructField(schema, StringType, nullable = true)
    }

    println(finalHiveSchema.size + " finalHiveSchema的字段个数")

    val rddArr: ArrayBuffer[String] = ArrayBuffer[String]()//存放rdd中的数据
    val rowArr =ArrayBuffer[Row]()//数据转为Row
    //    val finalDistict: Array[String] = finalHiveSchema.map(x =>(x.split("__")(0).toLowerCase(),1)).groupBy(_._1).filter(x => x._2.size > 1).keys.toArray//小写后相同的字段 eg:(taskId && taskid)
    for (message <- collectArr) {
      val propertiesKV: mutable.Map[String, String] = message._1._2//存放properties里的属性key和value

      //如果propertiesKV中有大小写字段，将其key处理
      val finalPropertiesKV = for (propertyKV <- propertiesKV) yield {
        if (finalHiveSchema.map(_.toLowerCase().split("__")(0)).contains(propertyKV._1.toLowerCase())) {//找出小写后相同的字段
        //将finalHiveSchema里的字段名赋予property
        val finalPropertyKey: String = finalHiveSchema.find(x =>x.split("__")(0).equals(propertyKV._1)).get
          finalPropertyKey -> propertyKV._2

        }else{
          propertyKV._1 -> propertyKV._2
        }
      }

      val properties: List[String] = message._1._2.keys.toList//properties里的属性keys
      //遍历finalHiveSchema,properties里有的字段将对应值加入rddArr,没有的值添加为null
      for (schema <- finalHiveSchema) {
        if (schema.equals("time")) rddArr.append(message._2._1)//time
        else if (schema.equals("event")) rddArr.append(message._2._2)//event
        else if (schema.equals("distinct_id")) rddArr.append(message._2._3)//distinct_id
        else if (schema.equals("user_id")) rddArr.append(message._2._4)//user_id
        else if (schema.equals("partition_date")) rddArr.append(DateUtil.getToday(message._2._1))//partition_date
        else if (finalPropertiesKV.keys.toArray.contains(schema)) {
          rddArr.append(finalPropertiesKV(schema))
        } else {
          rddArr.append("NULL")
        }
      }
      val row: Row = Row.fromSeq(rddArr)
      rowArr.append(row)
      rddArr.clear()//每一条消息结束后，清空rddArr
    }

    val rowRdd: RDD[Row] = sparkSession.sparkContext.parallelize(rowArr)
    val df: DataFrame = sqlContext.createDataFrame(rowRdd, StructType(finalHiveSchemaStructField))
    println(df.rdd.getNumPartitions + " 写入时df的分区")
    df.write.mode(SaveMode.Append).partitionBy("partition_date").saveAsTable("rawdata.events_increment")
    println("==========该批数据eventsJsonRdd分区导入完毕=========")

  }


  def getDfToKudu(time:String,user_id:String,map: mutable.Map[String, String], schema: StructType,sparkSession: SparkSession) = {
    //获取RDD[Row]
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    arr.append(time)
    arr.append(user_id)

    for (elem <- map) {
      arr.append(elem._2)
    }
    val sqlContext = new SQLContext(sparkSession.sparkContext)
    val row: Row = Row.fromSeq(arr)
    val rowList = List(row)
    val rowRdd: RDD[Row] = sparkSession.sparkContext.parallelize(rowList)

    //获取dataframe
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def userJsonRddToKudu(userJsonRdd: RDD[((String, mutable.Map[String, String]), (String, String, mutable.Map[String, String]))], sparkSession: SparkSession) = {
    println("======userJsonRddToKudu======")
    val collectArr: Array[((String, mutable.Map[String, String]), (String, String, mutable.Map[String, String]))] = userJsonRdd.sortBy(x => x._2._1.toLong).collect()//按时间升序，profile操作具有顺序性
    for (x <- collectArr) {
      val keys: List[String] = x._1._2.keys.toList //属性List
      println("该userJsonRdd properties中有 " + keys.size + "个属性")
      val nonPropertiesList = List("time","user_id")

      //获取dataframe的schema
      val array: ArrayBuffer[StructField] = ArrayBuffer[StructField]()

      //追加固定属性
      for (column <- nonPropertiesList) {
        if (column.equals("user_id")) {
          array.append(StructField("id", StringType, nullable = false))
        } else {
          array.append(StructField(column, StringType, nullable = true))
        }
      }

      for (k <- keys) {
        array.append(StructField(k.toString, StringType, nullable = true)) //array里追加属性列
      }

      val listSchema: List[StructField] = array.toList
      val schema: StructType = StructType(listSchema)
      val time: String = x._2._1
      val id: String = x._2._2
      val map: mutable.Map[String, String] = x._1._2
      val primaryKeys = List("id")//users表的主键

      //---------------------分类别增删改users表字段-----------------------
      if (x._1._1.equals("profile_set")) {
        val userDf: DataFrame = getDfToKudu(time,id,map,schema,sparkSession)

        SparkKuduUtil.saveRddToKudu(sparkSession.sparkContext,userDf,"users",primaryKeys)
      }

      if (x._1._1.equals("profile_set_once")) {
        SparkKuduUtil.readFromKudu(sparkSession.sparkContext,"users").createOrReplaceTempView("users")
        val flag: Boolean = sparkSession.sql(
          s"""
              select * from users where id = $id
          """.stripMargin).rdd.isEmpty()
//        println(flag)
        if (flag) {
          val userDf: DataFrame = getDfToKudu(time,id,map,schema,sparkSession)
          SparkKuduUtil.saveRddToKudu(sparkSession.sparkContext,userDf,"users",primaryKeys)
        }
      }

      if (x._1._1.equals("profile_increment")) {
        SparkKuduUtil.readFromKudu(sparkSession.sparkContext,"users").createOrReplaceTempView("users")
        val resultDf = sparkSession.sql(
          s"""
              select * from users where id = $id
          """.stripMargin)
        for (propertiesKey <- keys) {
          try{
            val rows: Array[Row] = resultDf.select(propertiesKey).collect()
            for (row <- rows) {
              val value: String = row.getString(0)
              //判断数值类型
              try{
                val doubleValue: Double = value.toDouble
                map(propertiesKey) = (map(propertiesKey).toDouble + doubleValue).toString
              }catch{
                case ex:Exception => println(propertiesKey + " 字段类型非double")
              }
            }
          }catch {
            case ex:Exception =>
              println("users表中没有字段 " + propertiesKey + " 添加该字段，并设置为 0")
              map(propertiesKey) = "0"
          }finally {
            val userDf: DataFrame = getDfToKudu(time,id,map,schema,sparkSession)
            SparkKuduUtil.saveRddToKudu(sparkSession.sparkContext,userDf,"users",primaryKeys)
          }
        }
      }

      if (x._1._1.equals("profile_delete")) {
        KuduAPI.deleteUserFromKudu("users",id)
      }

      if (x._1._1.equals("profile_append")) {
        SparkKuduUtil.readFromKudu(sparkSession.sparkContext,"users").createOrReplaceTempView("users")
        val resultDf = sparkSession.sql(
          s"""
              select * from users where id = $id
          """.stripMargin)
        for (propertiesKey <- keys) {
          val rows: Array[Row] = resultDf.select(propertiesKey).collect()
          for (row <- rows) {
            val value: String = row.getString(0)
            map(propertiesKey) = value.substring(0,value.length - 1) + "," + map(propertiesKey).substring(1,map(propertiesKey).length)
          }
        }

        val userDf: DataFrame = getDfToKudu(time,id,map,schema,sparkSession)
        SparkKuduUtil.saveRddToKudu(sparkSession.sparkContext,userDf,"users",primaryKeys)
      }

      if (x._1._1.equals("profile_unset")) {
        for (propertiesKey <- keys) {
          map(propertiesKey) = "null"
        }
        val userDf: DataFrame = getDfToKudu(time,id,map,schema,sparkSession)
        SparkKuduUtil.saveRddToKudu(sparkSession.sparkContext,userDf,"users",primaryKeys)
      }
    }
  }

  def trackJsonRddToKudu(trackJsonRdd: RDD[((String, mutable.Map[String, String]), (String,String, String, String, String, mutable.Map[String, String]))], sparkSession: SparkSession) = {
    println("======trackJsonRddToKudu======")
    val sqlContext = new SQLContext(sparkSession.sparkContext)
    val keyCountMap: collection.Map[(String, mutable.Map[String, String]), Long] = trackJsonRdd.countByKey()//这里是考虑同一批拉过来的数据可能有两种properties
    val keyInterator: List[(String, mutable.Map[String, String])] = keyCountMap.keys.toList//存放type类型和该批次所有属性map
    val nonPropertiesList = List("time", "event", "distinct_id", "user_id")
    if (keyInterator.size >= 1) {
      for (key <- keyInterator) {
        println("该trackJsonRdd properties中有 " + key._2.keys.size + "个属性")

        //属性1 filterRdd
        val filterRdd: RDD[((String, mutable.Map[String, String]), (String,String, String, String, String,mutable.Map[String, String]))] = trackJsonRdd.filter(x => {
          x._1._2.equals(key._2)
        })

        //获取eventsDataframe的schema
        val array: ArrayBuffer[StructField] = ArrayBuffer[StructField]()
        val list: List[String] = key._2.keys.toList

        //追加4个属性
        for (column <- nonPropertiesList) {
          if (column.equals("time") || column.equals("user_id")) {
            array.append(StructField(column, StringType, nullable = false))
          } else {
            array.append(StructField(column, StringType, nullable = true))
          }

        }

        for (k <- list) {
          array.append(StructField(k.toString, StringType, nullable = true)) //array里追加属性列
        }

        val listSchema: List[StructField] = array.toList

        val schema: StructType = StructType(listSchema)

        //获取eventsRDD[Row]
        val rowRdd: RDD[Row] = filterRdd.map(x => {
          val map: mutable.Map[String, String] = x._2._6
          val arr: ArrayBuffer[String] = ArrayBuffer[String]()
          arr.append(x._2._1)
          arr.append(x._2._2)
          arr.append(x._2._3)
          arr.append(x._2._4)
          for (elem <- map) {
            arr.append(elem._2)
          }
          val row: Row = Row.fromSeq(arr)
          row
        })

        //将rdd存入kudu
        val trackDf: DataFrame = sqlContext.createDataFrame(rowRdd, schema)
        val primaryKeys = List("user_id","time")
        SparkKuduUtil.saveRddToKudu(sparkSession.sparkContext, trackDf, "events",primaryKeys)

        //获取users表的schema
        val usersArray: ArrayBuffer[StructField] = ArrayBuffer[StructField]()
        usersArray.append(StructField("id", StringType, nullable = false))
        usersArray.append(StructField("first_id", StringType, nullable = true))
        usersArray.append(StructField("second_id", StringType, nullable = true))
        usersArray.append(StructField("time", StringType, nullable = true))

        val usersListSchema: List[StructField] = usersArray.toList
        val usersSchema: StructType = StructType(usersListSchema)

        //获取usersRDD[Row]
        val usersRowRdd: RDD[Row] = filterRdd.map(x => {
          val arr: ArrayBuffer[String] = ArrayBuffer[String]()
          arr.append(x._2._4)
          arr.append(x._2._3)
          arr.append(x._2._5)
          arr.append(x._2._1)
          val row: Row = Row.fromSeq(arr)
          row
        })
        //将usersRdd存入kudu
        val usersTrackDf: DataFrame = sqlContext.createDataFrame(usersRowRdd, usersSchema)
        val usersPrimaryKeys = List("id")
        SparkKuduUtil.saveRddToKudu(sparkSession.sparkContext, usersTrackDf, "users",usersPrimaryKeys)
      }
    }
  }

}

