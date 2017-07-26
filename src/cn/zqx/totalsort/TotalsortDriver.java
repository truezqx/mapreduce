package cn.zqx.totalsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalsortDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setMapperClass(TotalsortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(TotalsortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(3);
		job.setPartitionerClass(TotalsortPartitioner.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.21.128:9000/totalsort"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.21.128:9000/totalsort/result"));
		job.waitForCompletion(true);

	}

}
