package org.training.hadoop.hbase;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HBaseDemo {

    //与HBase数据库的连接对象
    Connection connection;
    //数据库元数据操作对象
    Admin admin;


    @Before
    public void setUp() throws IOException {
        //取得一个数据库连接的配置参数对象
        Configuration conf = HBaseConfiguration.create();
        //设置连接参数：HBase数据库所在主机IP
        conf.set("hbase.zookeeper.quorum", "192.168.170.100");

        //设置连接参数：HBase连接使用端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        //取得一个数据库连接对象
        connection = ConnectionFactory.createConnection(conf);
        //取得一个数据库元数据操作对象
        admin = connection.getAdmin();

    }

    @Test
    public void createTable() throws IOException {
        System.out.println("--------创建表----------");
        String tableNameString = "t_book";
        //新建一个数据表表名
        TableName tableName = TableName.valueOf(tableNameString);

        if (admin.tableExists(tableName)) {
            System.out.println("表已存在");
        } else {
            //数据表描述对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor family = new HColumnDescriptor("info");
            hTableDescriptor.addFamily(family);
            admin.createTable(hTableDescriptor);

        }

    }

    @Test
    public void insert() throws IOException {

        System.out.println("插入数据。。。");
        Table table = connection.getTable(TableName.valueOf("t_book"));
        ArrayList<Put> putList = new ArrayList<>();
        Put put;

        for (int i = 0; i < 10; i++) {
            put = new Put(Bytes.toBytes("row" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(String.valueOf(i)));
            putList.add(put);
        }
        table.put(putList);
    }

    @Test
    public void queryTable() throws IOException {
        System.out.println("查询整表数据。。。");

        Table table = connection.getTable(TableName.valueOf("t_book"));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            byte[] row = result.getRow();
            System.out.println("row key is " + new String(row));

            List<Cell> cellList = result.listCells();
            for (Cell cell : cellList) {

                String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                System.out.println("row values is : " + family + ":" + qualifier + " " + value);

            }
        }
    }

    @Test
    public void queryTableByRowKey() throws IOException {

        System.out.println("按键查询表数据");

        Table table = connection.getTable(TableName.valueOf("t_book"));

        Get get = new Get("row8".getBytes());
        Result result = table.get(get);
        byte[] row = result.getRow();
        System.out.println("row key" + Bytes.toString(row));

        while (result.advance()) {
            Cell cell = result.current();
            String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println("family:" + family + " qualifier:" + qualifier + " value:" + value);
        }

    }

    @Test
    public void queryTableByCondition() throws IOException {

        System.out.println("按条件查询。。。");
        Table table = connection.getTable(TableName.valueOf("t_book"));

        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareFilter.CompareOp.GREATER, Bytes.toBytes("6"));

        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {

            byte[] row = result.getRow();
            System.out.println("rowkey:" + new String(row));

            while (result.advance()) {
                Cell cell = result.current();

                String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("family:" + family + " qualifier:" + qualifier + " value:" + value);

            }

        }

    }


    @Test
    public void truncateTable() throws IOException {

        TableName tableName = TableName.valueOf("t_book");
        admin.disableTable(tableName);
        admin.truncateTable(tableName, true);

    }

    @Test
    public void deleteByRowKey() throws IOException {
        Table table = connection.getTable(TableName.valueOf("t_book"));
        Delete delete = new Delete(Bytes.toBytes("row2"));
        table.delete(delete);
    }


}
