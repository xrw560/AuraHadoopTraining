package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName User2BasicMapReduce
 * @Author nick
 * @Date 2019/2/27 14:16
 * @Description
 */
public class User2BasicMapReduce extends Configured implements Tool {

    //Mapper Class
    public static class ReadUserMapper extends TableMapper<Text, Put> {
        private Text mapOutputKey = new Text();

        @Override
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            // get rowkey
            String rowkey = Bytes.toString(key.get());

            // set
            mapOutputKey.set(rowkey);

            Put put = new Put(key.get());

            //iterator
            for (Cell cell : value.rawCells()) {
                // add family: info
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    // add column: name
                    if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        put.add(cell);
                    }
                    // add column: age
                    if ("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        put.add(cell);
                    }
                }
            }

            context.write(mapOutputKey, put);
        }
    }

    //Reducer Class
    public static class WriteBasicReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
        @Override
        public void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

            for (Put put : values) {
                context.write(null, put);
            }
        }
    }

    //Driver
    @Override
    public int run(String[] args) throws Exception {

        // create job
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());

        // set run job class
        job.setJarByClass(this.getClass());

        // set job
        Scan scan = new Scan();
        scan.setCaching(500);// 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false); // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                "user",// input HBase table name
                scan,// Scan instance to control CF and attribute selection
                ReadUserMapper.class, // mapper class
                Text.class,// mapper output key
                Put.class,// mapper output value
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                "basic",// output table
                WriteBasicReducer.class,// reducer class
                job
        );
        job.setNumReduceTasks(1);//at least one, adjust as required

        // submit job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        // get configuration
        Configuration configuration = HBaseConfiguration.create();

        // submit job
        int status = ToolRunner.run(configuration, new User2BasicMapReduce(), args);

        //exit program
        System.exit(status);

    }
}
