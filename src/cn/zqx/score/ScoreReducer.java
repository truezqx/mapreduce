package cn.zqx.score;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text, ScoreBean, Text, ScoreBean> {
	@Override
	protected void reduce(Text key, Iterable<ScoreBean> values,
			Reducer<Text, ScoreBean, Text, ScoreBean>.Context context) throws IOException, InterruptedException {
		ScoreBean sb = new ScoreBean();
		int chinese = 0;
		int english = 0;
		int math = 0;
		for (ScoreBean value : values) {
			chinese = chinese + value.getChinese();
			english = english + value.getEnglish();
			math = math + value.getMath();
		}
		sb.setName(key.toString());
		sb.setChinese(chinese);
		sb.setEnglish(english);
		sb.setMath(math);
		context.write(key, sb);
	}
}
