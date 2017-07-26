package cn.zqx.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.zqx.distinct.DistinctReducer;

public class FlowDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setMapperClass(FlowMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setReducerClass(FlowReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		job.setNumReduceTasks(3);
		job.setPartitionerClass(FlowPationer.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.21.128:9000/flow"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.21.128:9000/flow/result"));
		job.waitForCompletion(true);

	}

}
