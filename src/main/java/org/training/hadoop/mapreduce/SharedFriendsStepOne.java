package org.training.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * 以下是qq的好友列表数据，冒号前是一个用，冒号后是该用户的所有好友（数据中的好友关系是单向的）
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * C:F,A,D,I
 * D:A,E,F,L
 * E:B,C,D,M,L
 * F:A,B,C,D,E,O,M
 * G:A,C,D,E,F
 * H:A,C,D,E,O
 * I:A,O
 * J:B,O
 * K:A,C,D
 * L:D,E,F
 * M:E,F,G
 * O:A,H,I,J
 * <p>
 * 求出哪些人两两之间有共同好友，及他俩的共同好友都有谁？
 */
public class SharedFriendsStepOne {

    public static class SharedFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //A:B,C,D,E,F,O
            String line = value.toString();
            String[] person_friends = line.split(":");
            String person = person_friends[0];
            String friends = person_friends[1];
            for (String friend : friends.split(",")) {
                //输出<好友，人>
                context.write(new Text(friend), new Text(person));
            }
        }
    }

    public static class SharedFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text person : persons) {
                sb.append(person).append(",");
            }
            context.write(friend, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendsStepOne.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SharedFriendsStepOneMapper.class);
        job.setReducerClass(SharedFriendsStepOneReducer.class);

        FileInputFormat.setInputPaths(job, new Path("D:/srcdata/friends"));
        FileOutputFormat.setOutputPath(job, new Path("D:/temp/out"));

        job.waitForCompletion(true);
    }


} 