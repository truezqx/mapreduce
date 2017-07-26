package cn.zqx.flow;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split(" ");
		FlowBean flow = new FlowBean();
		flow.setPhone(data[0]);
		flow.setAddr(data[1]);
		String name = data[2];
		flow.setName(name);
		flow.setFlow(Integer.parseInt(data[3]));
		context.write(new Text(name), flow);
	}
}
