package cn.zqx.totalsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TotalsortPartitioner extends Partitioner<IntWritable, IntWritable> {

	@Override
	public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
		if (key.toString().matches("[0-9]") | key.toString().matches("[0-9]{2}")) {
			return 0;
		} else if (key.toString().matches("[0-9]{3}")) {
			return 1;
		} else {
			return 2;
		}

	}

}
