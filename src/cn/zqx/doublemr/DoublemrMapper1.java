package cn.zqx.doublemr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DoublemrMapper1 extends Mapper<LongWritable, Text, Text, Doublemr>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Doublemr>.Context context)
			throws IOException, InterruptedException {
		Doublemr dm = new Doublemr();
		String line = value.toString();
		String name = line.split(" ")[1];
		dm.setName(name);
		dm.setProfit(Integer.parseInt(line.split(" ")[2])-Integer.parseInt(line.split(" ")[3]));
		context.write(new Text(name), dm);
	}
}
