package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @ClassName Test
 * @Author nick
 * @Date 2018/11/27 9:41
 * @Description
 */
public class Test {

    static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.140:2181");
    }

    public static void createTable(String tableName, String[] family) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tableName)) {
            System.out.println("table exists");
            System.exit(0);
        } else {
            admin.createTable(desc);
            System.out.println("create table success");
        }
    }

    public static void main(String[] args) throws IOException {
        String tableName = "blog2";
        String[] family = {"article", "author"};
        createTable(tableName, family);
    }


}
