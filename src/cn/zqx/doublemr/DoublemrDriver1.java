package cn.zqx.doublemr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DoublemrDriver1 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setMapperClass(DoublemrMapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Doublemr.class);
		job.setReducerClass(DoublemrReducer1.class);
		job.setOutputKeyClass(Doublemr.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.21.128:9000/doublemr"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.21.128:9000/doublemr/result"));
		job.waitForCompletion(true);

	}

}
