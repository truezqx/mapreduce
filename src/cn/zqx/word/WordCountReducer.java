package cn.zqx.word;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer组件会接受到mapper组件输出的结果（mapper的context.write）
 * 第一个形参类型：mapper输出的key类型
 * 第二个形参类型：mapper输出的value类型
 * 三和四是用户自定义的
 * 
 * 注意：
 * 1.对于MR，reducer组件可以单独工作，但是必须依赖mapper组件
 * 2.引入reducer组件之后，输出的结果文件就是reducer组件的输出结果
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int result = 0;
		for(IntWritable value:values){
			//IntWritable转基本数据类型:调用get方法
			result = result+value.get();
		}
		context.write(key, new IntWritable(result));
	}
}
