package utils

import org.apache.kudu.client.{Delete, KuduClient, KuduSession, KuduTable}

object KuduAPI {

  val kuduMaster: String = PropertiesUtils.loadProperties("kuduMaster")

  def getConnection() ={
    new KuduClient.KuduClientBuilder(kuduMaster).build()
  }

  def deleteUserFromKudu(tableName:String,id:String) ={
    val client: KuduClient = getConnection()
    val kuduSession: KuduSession = client.newSession()
    val table: KuduTable = client.openTable(tableName)
    val delete: Delete = table.newDelete()
    delete.getRow.addString("id",id)
    println("成功刪除主键id " + id)
    kuduSession.apply(delete)
    kuduSession.close()
  }

  def deleteTableFromKudu(tableName:String) = {
    val client: KuduClient = getConnection()
    val kuduSession: KuduSession = client.newSession()
    if (client.tableExists(tableName)) {
      client.deleteTable(tableName)
      println("删除表 " + tableName)
    }

  }
}
