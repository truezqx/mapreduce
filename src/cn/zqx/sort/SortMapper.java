package cn.zqx.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, SortBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		SortBean sb = new SortBean();
		sb.setName(line.split(" ")[0]);
		sb.setScore(Integer.parseInt(line.split(" ")[1]));
		context.write(sb, NullWritable.get());
	}
}
