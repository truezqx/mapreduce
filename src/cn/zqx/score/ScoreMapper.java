package cn.zqx.score;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, ScoreBean> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ScoreBean>.Context context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) context.getInputSplit();
		String fileName = split.getPath().getName();
		String line = value.toString();
		String name = line.split(" ")[1];
		int score = Integer.parseInt(line.split(" ")[2]);
		ScoreBean sb = new ScoreBean();
		sb.setName(name);
		if (fileName.equals("chinese.txt")) {
			sb.setChinese(score);
		}
		if (fileName.equals("english.txt")) {
			sb.setEnglish(score);
		}
		if (fileName.equals("math.txt")) {
			sb.setMath(score);
		}
		context.write(new Text(name), sb);

	}
}
