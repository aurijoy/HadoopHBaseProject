package hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Map2phase2 {
	
	public static class Mapper2 extends TableMapper<Text, Text>  {
		private Text key1 = new Text();//set as the column of A or row of B.
		private Text value1 = new Text(); //set as the rest element of each line.
		public void map(ImmutableBytesWritable rowKey, Result columns, Context context) 
				throws IOException, InterruptedException {	
			byte[] stock = columns.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name"));
			byte[] xi = columns.getValue(Bytes.toBytes("xi"), Bytes.toBytes("xi"));
			key1.set(stock);
			value1.set(xi);			
			
		context.write(key1, value1);
		}
	}

	
	public static class Reducer2 extends TableReducer<Text, Text, ImmutableBytesWritable>{
		private Text key2 = new Text();
		private Text value2 = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			double sum=0;
			double count=0;
			double data=0;
			ArrayList<Double> list_val=new ArrayList<Double>();
			for (Text value:values){
				data = Double.parseDouble(value.toString());
				list_val.add(data);
				sum+=data;
				count++;
			}
			double mean = (sum)/(count);
			double std_dev=0,inter=0;
			for (Double double1 : list_val) {
				inter=0;
				inter=double1-mean;
				inter *= inter;
				std_dev += inter; 
			}
			double volatility = (Math.sqrt((std_dev)/(list_val.size()-1)));
			String volatile_price = Double.toString(volatility);
			// create hbase put with rowkey as date
			Put insHBase = new Put(key.getBytes());
			// insert sum value to hbase 
			insHBase.add(Bytes.toBytes("xbar"), Bytes.toBytes("xbar"), Bytes.toBytes(volatile_price));
			insHBase.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), Bytes.toBytes(key.toString()));
			// write data to Hbase table
			context.write(new ImmutableBytesWritable(key.getBytes()), insHBase);
				
		}
	}

}
