package utils;

import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.serializer.KryoRegistrator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: HbaseDemo
 * @description: HbaseUtils(Kerberos)
 * @author: Mr.Xie
 * @create: 2018-11-22 14:16
 **/

public class HbaseUtils implements KryoRegistrator{

    public static Connection getConnection() throws Exception{
        Connection connection = null;
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hadoop.security.authentication" , "Kerberos");
            conf.set("hbase.zookeeper.quorum", "hadoop1-172-18-12-1:2181,hadoop2-172-18-12-2:2181,hadoop3-172-18-12-3:2181");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.security.authentication", "kerberos");
            connection = ConnectionFactory.createConnection(conf);

//            conf.set("kerberos.principal", "hbase@HADOOP.COM");
//            conf.addResource("src/main/resources/hbase-site.xml");
//            conf.set("keytab.file", "E:\\idea\\final\\src\\main\\resources\\HbaseConf\\hbase.keytab");
//            System.setProperty("java.security.krb5.conf","E:\\idea\\final\\src\\main\\resources\\HbaseConf\\krb5.conf");
//            UserGroupInformation.setConfiguration(conf);
//            UserGroupInformation.loginUserFromKeytab("hbase@HADOOP.COM",
//                    Thread.currentThread().getContextClassLoader().getResource("HbaseConf/hbase.keytab").getPath());
//            Admin admin = connection.getAdmin();
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("kerboers认证错误:" + e.getMessage(), e);
        }
        return  connection;
    }


    /*
     * 为表添加数据（适合知道有多少列族的固定表）
     *
     * @rowKey rowKey
     *
     * @tableName 表名
     *
     * @column1 第一个列族列表
     *
     * @value1 第一个列的值的列表
     *
     * @column2 第二个列族列表
     *
     * @value2 第二个列的值的列表
     */

    public static void addData(Connection connection,String rowKey, String tableName,
                               String[] column1, String[] value1) throws IOException {
        Table table = null;
        try {
            Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
            table = connection.getTable(TableName.valueOf(tableName));// HTabel负责跟记录相关的操作如增删改查等//
            // 获取表
            HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                    .getColumnFamilies();

            for (int i = 0; i < columnFamilies.length; i++) {
                String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
                if (familyName.equals("info")) { // article列族put数据
                    for (int j = 0; j < column1.length; j++) {
                        put.addColumn(Bytes.toBytes(familyName),
                                Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                    }
                }
            }
            table.put(put);
            System.out.println("表"+tableName +"add data Success!");
        }catch (IOException e) {
            e.printStackTrace();
        } finally {
            table.close();
        }
    }


    /*
     * 根据rwokey查询
     *
     * @rowKey rowKey
     *
     * @tableName 表名*/


    public static Result getByRowKey(Connection connection,String tableName, String rowKey)
            throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        Result result = table.get(get);
        return result;
    }


    public static void insertData(Connection connection,String tableName,List<Put> list){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            table.put(list);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("批量添加数据成功");

    }

    /**
     *
     * @param tableName
     * @param rowKey_like  rowKey的模糊查询
     * @param familyName
     * @param cols
     * @return
     */
    public static List<Result> getRowsByColumns(Connection connection,String tableName, String rowKey_like, String familyName, String cols[]) throws IOException {
        Table table = null;
        Result result = null;
        List<Result> list = new ArrayList<Result>();
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            //rowkey的模糊匹配
            PrefixFilter filter = new PrefixFilter(rowKey_like.getBytes());
            Scan scan = new Scan();
            for (int i = 0; i < cols.length; i++) {
                scan.addColumn(familyName.getBytes(), cols[i].getBytes());
            }
            scan.setFilter(filter);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result rs : resultScanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            table.close();
        }
        return list;
    }
    /*
    从表中获取指定列的值
    */
    public static Result getRowsByColumn(Connection connection,String tableName, String rowKey,
                                         String familyName, String columnName) throws IOException {
        Table table = null;
        Result result = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
            result = table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            table.close();
        }
        return result;
    }

    /**
     * 删除数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void delDByRowKey(Connection connection,String tableName, String rowKey) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("删除 "+tableName+" 的 "+rowKey+" success!");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
        }
    }


    /**
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param value
     * @throws IOException
     */
    public static void updateTable(Connection connection,String tableName, String rowKey,
                                   String familyName, String columnName, String value)
            throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(value));
            table.put(put);
            System.out.println("update table"+tableName+ "Success!");
        }catch (IOException e) {
            e.printStackTrace();
        } finally {
            table.close();
        }
    }

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(HbaseUtils.class);
    }


//    // 关闭连接
//    public static void close() {
//        try {
//            if (admin != null) {
//                admin.close();
//            }
//            if (null != connection) {
//                connection.close();
//            }
//            if (table != null) {
//                table.close();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

}
