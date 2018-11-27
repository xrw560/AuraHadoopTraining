package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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
        conf.set("hbase.zookeeper.quorum", "192.168.1.140");

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
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(String.valueOf(i + "str")));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(20 + i));
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

        Get get = new Get("row0".getBytes());
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
    public void valueFilter() throws IOException {

        Table table = connection.getTable(TableName.valueOf("t_book"));

        Scan scan = new Scan();
//        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("6"));
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, new SubstringComparator("6"));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);

        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            System.out.println(name);
        }
        rs.close();

    }

    @Test
    public void multiFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("t_book"));
        Scan scan = new Scan();

        //创建过滤器列表
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        //只有列族为info的记录才放入结果集
        Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info")));
        filterList.addFilter(familyFilter);

        //只有列为name的记录才放入结果集
        Filter colFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("name")));
        filterList.addFilter(colFilter);

        Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("6"));
        filterList.addFilter(valueFilter);

        scan.setFilter(filterList);

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            System.out.println(name);
        }
        rs.close();
    }

    @Test
    public void multiFilter2() throws IOException {

        String family = "info";
        Table table = connection.getTable(TableName.valueOf("t_book"));
        Scan scan = new Scan();

        //创建内层FilterList, 设置运算符为OR
        FilterList innerFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

        //找出
        Filter in1Filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes("city"), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("xiamen")));
        innerFilterList.addFilter(in1Filter);

        Filter in2Filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes("city"), CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("shanghai")));
        innerFilterList.addFilter(in2Filter);

        //创建外层FilterList, 设置运算符为AND
        FilterList outerFilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //将内层过滤器列表作为外层过滤器列表的第一个过滤器
        outerFilterList.addFilter(outerFilterList);

        //设置过滤条件为active='1
        Filter activeFilter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes("active"), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("1")));
        outerFilterList.addFilter(activeFilter);

        scan.setFilter(outerFilterList);

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String city = Bytes.toString(r.getValue(Bytes.toBytes(family), Bytes.toBytes("city")));
            System.out.println(city);
        }
        rs.close();

    }


    @Test
    public void pageFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("t_book"));

        Scan scan = new Scan();
        //设置分页为每页2条数据
        Filter filter = new PageFilter(2);
        scan.setFilter(filter);

        //第一页
        ResultScanner rs = table.getScanner(scan);

        byte[] lastRowKey = printResult(rs);
        rs.close();
        System.out.println("现在打印第2页");
        //为lastRowKey拼接上一个零字节
        byte[] startRowKey = Bytes.add(lastRowKey, new byte[1]);
        scan.setStartRow(startRowKey);
        ResultScanner rs2 = table.getScanner(scan);
        printResult(rs2);
        rs2.close();

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

    @Test
    public void countTable() throws Throwable {
        /**
         * https://blog.csdn.net/m0_37739193/article/details/75286496
         * 启用表aggregation，只对特定的表生效。通过hbase Shell 来实现。
         * 1. disable 't_book'
         * 2. alter 't_book', METHOD => 'table_att','coprocessor'=>'|org.apache.hadoop.hbase.coprocessor.AggregateImplementation||'
         * 3. enable 't_book'
         * 4. describe 't_book'
         */
        Configuration configuration = connection.getConfiguration();
        AggregationClient aggregationClient = new AggregationClient(configuration);
        Scan scan = new Scan();
        long rowCount = aggregationClient.rowCount(TableName.valueOf("t_book"), new LongColumnInterpreter(), scan);
        System.out.println(rowCount);

    }


    private static byte[] printResult(ResultScanner rs) {
        byte[] lastRowKey = null;
        for (Result r : rs) {
            byte[] rowkey = r.getRow();
            String name = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            int age = Bytes.toInt(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
            System.out.println(Bytes.toString(rowkey) + ": name=" + name + " age=" + age);
            lastRowKey = rowkey;
        }
        return lastRowKey;
    }

}
