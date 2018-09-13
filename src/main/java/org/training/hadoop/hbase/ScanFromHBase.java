package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanFromHBase {

    /**
     * 读取多行数据
     */
    public static void scan(Configuration conf) throws IOException {
        Connection connection = null;

        connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(TableInformation.TABLE_NAME));

        Scan scan = new Scan();
        //设置起始和结束⾏键key， [start_key, stop_key)
        scan.setStartRow(Bytes.toBytes("row030"));
        scan.setStopRow(Bytes.toBytes("row039"));
        //加⼊需要访问的列
        scan.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_1));
        scan.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_2));
        //设置每次返回的结果数量
        scan.setCaching(100);
        //发送请求， 获得ResultScanner句柄
        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            while (result.advance()) {
                System.out.println(result.current());
            }
        }
        table.close();
        connection.close();
    }

    public static void main(String args[]) throws IOException {
        ScanFromHBase.scan(TableInformation.getHBaseConfiguration());
    }
}
