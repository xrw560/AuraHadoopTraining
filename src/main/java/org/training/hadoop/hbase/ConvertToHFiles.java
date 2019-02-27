package org.training.hadoop.hbase;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @ClassName ConvertToHFiles
 * @Author nick
 * @Date 2019/2/27 19:00
 * @Description
 */
public class ConvertToHFiles extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(ConvertToHFiles.class);

    public static class ConvertToHFilesMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Cell> {

        public static final byte[] CF = Bytes.toBytes("info");
        public static final ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
        static ArrayList<byte[]> qualifiers = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            context.getCounter("Convert", "mapper").increment(1);

            //列的字段，这里是三列
            //"rowkey", "name", "age", "sex", "address", "phone"
            byte[] name = Bytes.toBytes("name");
            byte[] age = Bytes.toBytes("age");
            byte[] sex = Bytes.toBytes("sex");
            byte[] address = Bytes.toBytes("address");
            byte[] phone = Bytes.toBytes("phone");
            qualifiers.add(name);
            qualifiers.add(age);
            qualifiers.add(sex);
            qualifiers.add(address);
            qualifiers.add(phone);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            //字段以逗号分割
            String[] line = value.toString().split(",");

            byte[] rowKeyBytes = Bytes.toBytes(line[0]);
            rowKey.set(rowKeyBytes);

            context.getCounter("Convert", line[2]).increment(1);

            for (int i = 0; i < line.length - 1; i++) {
                KeyValue kv = new KeyValue(rowKeyBytes, CF, qualifiers.get(i), Bytes.toBytes(line[i + 1]));

                if (null != kv) {
                    context.write(rowKey, kv);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(args[0]);
        System.out.println(args[1]);
        System.out.println(args[2]);
        Configuration configuration = HBaseConfiguration.create();
        int res = ToolRunner.run(configuration, new CSV2HFileMapReduce(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            System.out.println(args[0]);
            System.out.println(args[1]);
            System.out.println(args[2]);

            String inputPath = args[1];
            String outputPath = args[2];
            final TableName tableName = TableName.valueOf(args[0]);


            //create hbase connection
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName);

            //create job
            Job job = Job.getInstance(conf, "ConvertToHFiles: Convert File to HFiles");
            job.setInputFormatClass(TextInputFormat.class);
            job.setJarByClass(ConvertToHFiles.class);

            job.setMapperClass(ConvertToHFilesMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);

            HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName));

            FileInputFormat.setInputPaths(job, inputPath);
            HFileOutputFormat2.setOutputPath(job, new Path(outputPath));

            if (!job.waitForCompletion(true)) {
                LOG.error("Failure");
            } else {
                LOG.info("Success");
                return 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }
}
