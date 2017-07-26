package cn.zqx.doublemr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DoublemrReducer1 extends Reducer<Text, Doublemr, Doublemr, NullWritable>{
	@Override
	protected void reduce(Text key, Iterable<Doublemr> values,
			Reducer<Text, Doublemr, Doublemr, NullWritable>.Context context) throws IOException, InterruptedException {
		Doublemr dm = new Doublemr();
		dm.setName(key.toString());
		for(Doublemr value:values){
			dm.setProfit(value.getProfit()+dm.getProfit());
		}
		context.write(dm, NullWritable.get());
	}
}
