package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GetFromHBase {
    /**
     * 读取⼀⾏数据
     */
    public static void get(Configuration conf) throws IOException {
        Connection connection = null;

        connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(TableInformation.TABLE_NAME));
        //构造对象传⼊⾏键key
        Get get = new Get(Bytes.toBytes("row1"));
        //加⼊需要访问的列
        get.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_1));
        get.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_1));
        //发送请求， 获得Result
        Result result = table.get(get);
        //遍历结果
        while (result.advance()) {
            System.out.println(result.current());
        }
        table.close();
        connection.close();
    }

    public static void main(String[] args) throws IOException {
        GetFromHBase.get(TableInformation.getHBaseConfiguration());
    }
}
