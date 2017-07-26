package cn.zqx.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setMapperClass(ScoreMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreBean.class);
		job.setReducerClass(ScoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ScoreBean.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.21.128:9000/score"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.21.128:9000/score/result"));
		job.waitForCompletion(true);
	}

}
