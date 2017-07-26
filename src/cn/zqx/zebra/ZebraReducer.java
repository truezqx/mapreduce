package cn.zqx.zebra;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ZebraReducer extends Reducer<Text, HttpAppHost, HttpAppHost, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<HttpAppHost> values,
			Reducer<Text, HttpAppHost, HttpAppHost, NullWritable>.Context context)
			throws IOException, InterruptedException {
		HttpAppHost mapHah = new HttpAppHost();
		for (HttpAppHost hah : values) {
			mapHah.setReportTime(hah.getReportTime());
			// 上网小区的id
			mapHah.setCellid(hah.getCellid());
			// 应用类
			mapHah.setAppType(hah.getAppType());
			// 应用子类
			mapHah.setAppSubtype(hah.getAppSubtype());
			// 用户ip
			mapHah.setUserIP(hah.getUserIP());
			// 用户port
			mapHah.setUserPort(hah.getUserPort());
			// 访问的服务ip
			mapHah.setAppServerIP(hah.getAppServerIP());
			// 访问的服务port
			mapHah.setAppServerPort(hah.getAppServerPort());
			// 域名
			mapHah.setHost(hah.getHost());

			mapHah.setCellid(hah.getCellid());
			mapHah.setAccepts(mapHah.getAccepts() + hah.getAccepts());
			mapHah.setAttempts(mapHah.getAttempts() + hah.getAttempts());
			mapHah.setTrafficUL(mapHah.getTrafficUL() + hah.getTrafficUL());
			mapHah.setTrafficDL(mapHah.getTrafficDL() + hah.getTrafficDL());
			mapHah.setRetranUL(mapHah.getRetranUL() + hah.getRetranUL());
			mapHah.setRetranDL(mapHah.getRetranDL() + hah.getRetranDL());
			mapHah.setTransDelay(mapHah.getTransDelay() + hah.getTransDelay());
		}
		context.write(mapHah, NullWritable.get());
	}
}
