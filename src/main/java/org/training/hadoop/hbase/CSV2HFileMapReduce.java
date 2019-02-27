package org.training.hadoop.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * @Author nick
 * @Date 2019/2/27 18:09
 * @Description
 */
public class CSV2HFileMapReduce extends Configured implements Tool {

    public static final String COLUMN_FAMILY = "info";
    public static final String[] COLUMNS = new String[]{"rowkey", "name", "age", "sex", "address", "phone"};

    public static class ReadCSVMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private ImmutableBytesWritable mapOutputKey = new ImmutableBytesWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer token = new StringTokenizer(line, ",");
            String[] fields = new String[6];

            int i = 0;
            while (token.hasMoreTokens()) {
                fields[i++] = token.nextToken();
            }
            mapOutputKey.set(Bytes.toBytes(fields[0]));
            Put put = new Put(Bytes.toBytes(fields[0]));
            for (int index = 1; index < 6; index++) {
                System.out.println(fields[index]);
                if (StringUtils.isEmpty(fields[index])) {
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMNS[index]), Bytes.toBytes(fields[index]));
                }
            }
            context.write(mapOutputKey, put);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        job.setMapperClass(ReadCSVMapper.class);
//        job.setReducerClass(PutSortReducer.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        HFileOutputFormat2.configureIncrementalLoad(job, new HTable(getConf(), args[0]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int status = ToolRunner.run(configuration, new CSV2HFileMapReduce(), args);
        System.exit(status);
    }
}
