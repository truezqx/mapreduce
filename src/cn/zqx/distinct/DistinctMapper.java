package cn.zqx.distinct;

import java.io.IOException;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		context.write(value, NullWritable.get());
	}
}
