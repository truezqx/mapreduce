package cn.zqx.doublemr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Doublemr implements WritableComparable<Doublemr>{
	private String name;
	private int profit;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getProfit() {
		return profit;
	}
	public void setProfit(int profit) {
		this.profit = profit;
	}
	@Override
	public String toString() {
		return name + " " + profit;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(profit);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.profit = in.readInt();
		
	}
	@Override
	public int compareTo(Doublemr o) {
		return this.profit-o.profit;
	}
	
	
}
