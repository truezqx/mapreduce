package cn.zqx.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SortDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(SortBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(SortBean.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.21.128:9000/sort"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.21.128:9000/sort/result"));
		job.waitForCompletion(true);
	}

}
