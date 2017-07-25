package cn.zqx.max;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		long year = 0;
		long temperature = 0;
		String line = value.toString();
		year = Long.parseLong(line.substring(8, 12));
		temperature = Long.parseLong(line.substring(18));
		context.write(new LongWritable(year), new LongWritable(temperature));
	}

}
