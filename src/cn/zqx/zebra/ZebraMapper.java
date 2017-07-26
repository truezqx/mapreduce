package cn.zqx.zebra;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ZebraMapper extends Mapper<LongWritable, Text, Text, HttpAppHost> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, HttpAppHost>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split("\\|");
		FileSplit split = (FileSplit) context.getInputSplit();
		String reportTime = split.getPath().getName().split("_")[1];
		HttpAppHost hah = new HttpAppHost();
		hah.setReportTime(reportTime);
		// 上网小区的id
		hah.setCellid(data[16]);
		// 应用类
		hah.setAppType(Integer.parseInt(data[22]));
		// 应用子类
		hah.setAppSubtype(Integer.parseInt(data[23]));
		// 用户ip
		hah.setUserIP(data[26]);
		// 用户port
		hah.setUserPort(Integer.parseInt(data[28]));
		// 访问的服务ip
		hah.setAppServerIP(data[30]);
		// 访问的服务port
		hah.setAppServerPort(Integer.parseInt(data[32]));
		// 域名
		hah.setHost(data[58]);

		int appTypeCode = Integer.parseInt(data[18]);
		String transStatus = data[54];

		// 业务逻辑处理
		// 小区id如果没有，就补全9个0
		if (hah.getCellid() == null || hah.getCellid().equals("")) {
			hah.setCellid("000000000");
		}
		// 如果=103，尝试次数就设置为1
		if (appTypeCode == 103) {
			hah.setAttempts(1);
		}
		// 如果=103，并且状态码是低下的数字，接收次数就设置为1
		if (appTypeCode == 103
				&& "10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,199,200,201,202,203,204,205,206,302,304,306"
						.contains(transStatus)) {
			hah.setAccepts(1);
		} else {
			hah.setAccepts(0);
		}
		// 如果=103，就设置上传流量
		if (appTypeCode == 103) {
			hah.setTrafficUL(Long.parseLong(data[33]));
		}
		// 如果=103，就设置下传流量
		if (appTypeCode == 103) {
			hah.setTrafficDL(Long.parseLong(data[34]));
		}
		// 如果=103，设置重传上传流量
		if (appTypeCode == 103) {
			hah.setRetranUL(Long.parseLong(data[39]));
		}
		// 如果=103，设置重传下行流量
		if (appTypeCode == 103) {
			hah.setRetranDL(Long.parseLong(data[40]));
		}
		// 设置传输延时，请求终止时间-请求的起始时间。
		if (appTypeCode == 103) {
			hah.setTransDelay(Long.parseLong(data[20]) - Long.parseLong(data[19]));
		}
		String userKey = hah.getReportTime() + "|" + hah.getAppType() + "|" + hah.getAppSubtype() + "|"
				+ hah.getUserIP() + "|" + hah.getUserPort() + "|" + hah.getAppServerIP() + "|" + hah.getAppServerPort()
				+ "|" + hah.getHost() + "|" + hah.getCellid();

		context.write(new Text(userKey), hah);
	}
}
