package cn.zqx.max;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import sun.util.logging.resources.logging_zh_TW;

public class MaxReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values,
			Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		long result = 0;
		for(LongWritable value:values){
			if(value.get()>result){
				result = value.get();
			}
		}
		context.write(key, new LongWritable(result));
	}
}
