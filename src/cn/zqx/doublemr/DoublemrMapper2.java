package cn.zqx.doublemr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DoublemrMapper2 extends Mapper<LongWritable, Text, Doublemr, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Doublemr, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		Doublemr dm = new Doublemr();
		dm.setName(line.split(" ")[0]);
		dm.setProfit(Integer.parseInt(line.split(" ")[1]));
		context.write(dm, NullWritable.get());
	}
}
