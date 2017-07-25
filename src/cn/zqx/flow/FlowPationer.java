package cn.zqx.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPationer extends Partitioner<Text, FlowBean>{

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String city = value.getAddr();
		if(city.equals("bj")){
			return 0;
		}else if(city.equals("sh")){
			return 1;
		}else{
			return 2;
		}
		
	}

}
