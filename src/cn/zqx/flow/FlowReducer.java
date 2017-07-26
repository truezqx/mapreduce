package cn.zqx.flow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		FlowBean flow = null;
		for (FlowBean value : values) {
			flow = value;
			count = count + value.getFlow();
			flow.setFlow(count);
		}
		context.write(key, flow);
	}
}
