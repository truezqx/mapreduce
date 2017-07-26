package cn.zqx.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 设置job工作的运行类
		job.setJarByClass(WordCountDriver.class);
		// 设置Mapper组件的运行类
		job.setMapperClass(WordCountMapper.class);
		// 设置mapper组件输出的key类型
		job.setMapOutputKeyClass(Text.class);
		// 设置Mapper组件输出的value类型
		job.setMapOutputValueClass(IntWritable.class);
		// 设置reducer组件运行类
		job.setReducerClass(WordCountReducer.class);
		// 设置reducer输出的key类型
		job.setOutputKeyClass(Text.class);
		// 设置reducer输出的value类型
		job.setOutputValueClass(IntWritable.class);
		// 设置Job处理文件所在的HDFS路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.21.128:9000/wordcount"));
		// MR处理生成的结果是以文件形式来存储的，也需要放在指定的HDFS上
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.21.128:9000/wordcount/result"));
		job.waitForCompletion(true);
	}

}
