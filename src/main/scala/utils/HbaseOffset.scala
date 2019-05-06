package utils
import java.util
import java.util.Properties
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import scala.collection.JavaConversions._
import scala.collection.mutable
object HbaseOffset {
  /*
  Save offsets for each batch into HBase
  */
  def saveOffsets(TOPIC_NAME: String, GROUP_ID: String, offsetRanges: Array[OffsetRange],
                  hbaseTableName: String, batchTime: String) = {
    val conn: Connection = HbaseUtils.getConnection
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val rowKey = TOPIC_NAME + ":" + GROUP_ID + ":" + batchTime
    val put = new Put(rowKey.getBytes)
    for (offset <- offsetRanges) {
      put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes(offset.partition.toString),
        Bytes.toBytes(offset.untilOffset.toString))
      println(offset.partition.toString + " ===== " + offset.untilOffset.toString)
    }
    if (!put.isEmpty) {
      table.put(put)
      println("新偏移量保存成功" + "批次时间为 === " + batchTime)
    }
    conn.close()
  }


  /* Returns last committed offsets for all the partitions of a given topic from HBase in
   following  cases.
   */
  def getLastCommittedOffsets(TOPIC_NAME: String, GROUP_ID: String, hbaseTableName: String):Map[TopicPartition, Long] = {

    //该topic的分区数
    val zKNumberOfPartitionsForTopic = 10
    //Connect to HBase to retrieve last committed offsets

    //获取hbase的连接
    val conn: Connection = HbaseUtils.getConnection
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val startRow = TOPIC_NAME + ":" + GROUP_ID + ":" +
      String.valueOf(System.currentTimeMillis())
    val stopRow = TOPIC_NAME + ":" + GROUP_ID + ":" + 0
    val scan = new Scan()
    val scanner = table.getScanner(scan.setStartRow(startRow.getBytes).setStopRow(
      stopRow.getBytes).setReversed(true))
    val result = scanner.next()
    var hbaseNumberOfPartitionsForTopic: Int = 0 //Set the number of partitions discovered for a topic in HBase to 0
    if (result != null) {
      //If the result from hbase scanner is not null, set number of partitions from hbase to the number of cells
      hbaseNumberOfPartitionsForTopic = result.listCells().size()
    }

    //获取kafkaTopic中最小的offset
    val earliestFromOffsets: mutable.HashMap[TopicPartition, Long] = getEarliestOffsetOfKafka(TOPIC_NAME:String)

    //sparkstreaming_kafka消费开始的offset
    val fromOffsets = mutable.HashMap[TopicPartition, Long]()

    if (hbaseNumberOfPartitionsForTopic == 0) {
      // initialize fromOffsets to beginning
      //Streaming任务第一次启动，从zookeeper中获取给定topic的分区数，然后将每个分区的offset都设置为earliestOffset，并返回。
      for (partition <- 0 to zKNumberOfPartitionsForTopic - 1) {
        for (earliestOffset <- earliestFromOffsets) {
          fromOffsets += (earliestOffset._1 -> earliestOffset._2)
        }
      }
    } else if (zKNumberOfPartitionsForTopic > hbaseNumberOfPartitionsForTopic) {
      // handle scenario where new partitions have been added to existing kafka topic
      //一个运行了很长时间的streaming任务停止并且给定的topic增加了新的分区，处理方式是从zookeeper中获取给定topic的分区数，
      // 对于所有老的分区，offset依然使用HBase中所保存，对于新的分区则将offset设置为0。
      for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {
        val fromOffset = Bytes.toString(result.getValue(Bytes.toBytes("offsets"),
          Bytes.toBytes(partition.toString)))
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> fromOffset.toLong)
      }
      for (partition <- hbaseNumberOfPartitionsForTopic to zKNumberOfPartitionsForTopic - 1) {
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> 0)
      }
    } else {
      //initialize fromOffsets from last run
      //Streaming任务长时间运行后停止并且topic分区没有任何变化，在这个情形下，
      // 先判断hbase offset是否小于earlieset offset,如果小，hbase offset更新为earlieset offset
      // 否则直接使用HBase中所保存的offset即可。

      for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {
        val fromOffset: String = Bytes.toString(result.getValue(Bytes.toBytes("offsets"),
          Bytes.toBytes(partition.toString)))

        val map: mutable.HashMap[Int, Long] = earliestFromOffsets.map(x =>{(x._1.partition(),x._2)})

        if (fromOffset.toLong < map(partition)) {
          fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> map(partition))
        }else{
          fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> fromOffset.toLong)
        }
      }
    }
    scanner.close()
    conn.close()
    println("从Hbase中拿出历史偏移量")
    fromOffsets.toMap

  }

  //为了避免kafka数据清理导致的rangeException
  def getEarliestOffsetOfKafka(topicName: String) = {
    val bootstrap_servers: String = PropertiesUtils.loadProperties("bootstrap.servers")
    val groupId: String = "groupOfGetEarliesetOffset"
    val props = new Properties()
    //消费者配置
    props.put("bootstrap.servers",bootstrap_servers)
    props.put("group.id",groupId)
    props.put("enable.auto.commit", "false")//不提交offset,保证每次从最开始读取
    props.put("auto.offset.reset","earliest")//设置为earliest,每次都消费最初的offset
    props.put("max.poll.records", "1")//每个分区每次消费一条，提高效率
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    //新建一个消费者，在sparkstreaming取kafka偏移量之前先拿到每个分区目前最小的offset,避免因为kafka过期导致的offsetRangeException
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String,String](props)
    val partitionList: util.List[PartitionInfo] = consumer.partitionsFor(topicName)

    val list = new util.ArrayList[TopicPartition]()

    //存放各分区最小的offset
    val earliestFromOffsets = new mutable.HashMap[TopicPartition,Long]()

    for (partitionInfo <- partitionList) {
      val partition = new TopicPartition(topicName,partitionInfo.partition())
      list.add(partition)
    }


    while (earliestFromOffsets.size != list.size()) {
      //每次读取一个分区一条记录，当earliestFromOffsets与list size相同,即拿到所有分区最小offset
      for (i <- 0 until list.size()) {
        //指定消费分区
        consumer.assign(util.Collections.singletonList(list(i)))
        val records: ConsumerRecords[String, String] = consumer.poll(100)
        for (record <- records) {
          println(record.partition() + " " + record.offset())
        }

        val topicPartitionSet: mutable.Set[(TopicPartition, Long)] = records.partitions().map(topicPartition => {
          consumer.seekToBeginning(util.Collections.singletonList(topicPartition))
          val offset: Long = consumer.position(topicPartition)
          (topicPartition, offset)
        })

        if (!topicPartitionSet.isEmpty) {
          //拿到第一条记录
          val head: (TopicPartition, Long) = topicPartitionSet.toList.head
          earliestFromOffsets.put(head._1,head._2)
        }
        println("==============================")
      }

    }

    for (elem <- earliestFromOffsets) {
      println(elem._1.topic() + " " + elem._1.partition() + " " + elem._2)
    }
    earliestFromOffsets
  }
}

