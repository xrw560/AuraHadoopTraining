package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * @ClassName HBaseOperation
 * @Author nick
 * @Date 2019/2/27 9:12
 * @Description
 */
public class HBaseOperation {

    public static HTable getHTableByTaleName(String tableName) throws IOException {
        // Get instance of Default Configuration
        Configuration configuration = HBaseConfiguration.create();
        //Get  Table Instance
        HTable table = new HTable(configuration, tableName);
        return table;
    }

    public static void getData(HTable table) throws IOException {
        Get get = new Get(Bytes.toBytes("10002"));

        //add column
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));

        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + " : "
                    + Bytes.toString(CellUtil.cloneQualifier(cell)) + " -> "
                    + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
    }

    /**
     * 建议表的名称和列族都写成常量， HBaseTableContent
     * Map<String,Object>
     */
    public static void putData(HTable table) throws IOException {
        Put put = new Put(Bytes.toBytes("10004"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhaoliu"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(25));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes("beijing"));

        table.put(put);//可以写list<Put>
        System.out.println("Put Success...");
    }

    public static void delete(HTable table) throws IOException {
        Delete delete = new Delete(Bytes.toBytes("10004"));
        //不添加条件，表示删除行
//        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"));
        delete.addFamily(Bytes.toBytes("info"));
        table.delete(delete);
    }


    public static void main(String[] args) throws IOException {
        String tableName = "user";

        HTable table = null;
        ResultScanner scanner = null;

        try {
            table = getHTableByTaleName(tableName);

            Scan scan = new Scan();

            scan.setStartRow(Bytes.toBytes("10001"));//包含
            scan.setStopRow(Bytes.toBytes("10003"));//不包含
//            Scan scan2 = new Scan(Bytes.toBytes("10001"),(Bytes.toBytes("10003"));
//            scan.addColumn()
//            scan.addFamily()
            // PrefixFilter
            // PageFilter
//            scan.setFilter()
//            scan.setCacheBlocks();
//            scan.setCaching()
            scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
//                System.out.println(result);
                for (Cell cell : result.rawCells()) {
                    System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + " : "
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + " -> "
                            + Bytes.toString(CellUtil.cloneValue(cell))
                    );
                }
                System.out.println("--------------------------------");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(scanner);
            IOUtils.closeStream(table);
        }
    }
}
