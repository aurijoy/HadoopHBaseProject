package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Map1phase1 {
public static class Mapper1 extends TableMapper<Text, Text> {
	 
	 public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
	   throws IOException, InterruptedException {

	  try {
	   byte[] yr =columns.getValue(Bytes.toBytes("time"), Bytes.toBytes("yr"));
	   byte[] mm =columns.getValue(Bytes.toBytes("time"), Bytes.toBytes("mm"));
	   byte[] dd =columns.getValue(Bytes.toBytes("time"), Bytes.toBytes("dd"));
	   
	   byte[] price = columns.getValue(Bytes.toBytes("price"), Bytes.toBytes("price"));
	   byte[] stock = columns.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name"));
	   
	   String oKey = Bytes.toString(stock)+"-"+Bytes.toString(yr)+"-"+Bytes.toString(mm);
	   String oVal = Bytes.toString(dd)+"-"+Bytes.toString(price);
	   
	   System.out.println("Data is "+oKey +" "+oVal);
	   context.write(new Text(oKey), new Text(oVal));
	  } catch (RuntimeException e){
	   e.printStackTrace();
	  }
	 }
	}

public static class Reducer1 extends TableReducer<Text, Text, ImmutableBytesWritable>{
	 
	 public void reduce(Text key, Iterable<Text> values, Context context) 
	   throws IOException, InterruptedException {
	
		ArrayList<String> day = new ArrayList<String>();
		HashMap<String, String> day_value = new HashMap<String, String>();
		String test=null;
		String data[] = new String[2];
		String  low = null,high = null;
		int count=0;
		for (Text text : values) {			
			test=text.toString();
			data=test.split("-");
			System.out.println("Data is "+test);
			if(count==0)
			{
				low=data[0];
				high=data[0];
				count=1;
			}
			day.add(data[0]); //data[0] is month data[1] is price
			
			if(low.compareTo(data[0])>0)
			{
				low=data[0];
			}
			if(high.compareTo(data[0])<0)
			{
				high = data[0];
			}
			day_value.put(data[0], data[1]);					
		}
		

		double beg_price,last_price,stock_volatile_price;
		//Collections.sort(day);
		
		beg_price=Double.parseDouble(day_value.get(low));				
		last_price= Double.parseDouble(day_value.get(high));
		stock_volatile_price = (last_price-beg_price)/(beg_price);
		data = key.toString().split("-"); 
		String set_key=null,set_value=null;
		set_key =data[0];
		String volatile_price = Double.toString(stock_volatile_price);
		set_value=volatile_price;	   	  
	  		
	   // create hbase put with rowkey as date
	   Put insHBase = new Put(key.getBytes());
	   // insert sum value to hbase 
	   insHBase.add(Bytes.toBytes("xi"), Bytes.toBytes("xi"), Bytes.toBytes(set_value));
	   insHBase.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), Bytes.toBytes(set_key));
	   // write data to Hbase table
	   context.write(new ImmutableBytesWritable(key.getBytes()), insHBase);
	 }
	}
}
