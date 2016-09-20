package hbase;
import hbase.Map1phase1.Mapper1;
import hbase.Map1phase1.Reducer1;
import hbase.Map2phase2.Mapper2;
import hbase.Map2phase2.Reducer2;
import hbase.Map3phase3.Mapper3;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	public static void main(String[] args) throws Exception {		
		long start = new Date().getTime();		
		//Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		
		
		
		 Configuration config = HBaseConfiguration.create();
		 
		 HBaseAdmin admin = new HBaseAdmin(config);
		 HTableDescriptor tableDescriptor = new HTableDescriptor("reduce1");
		 tableDescriptor.addFamily(new HColumnDescriptor("xi"));
		 tableDescriptor.addFamily(new HColumnDescriptor("stock"));
		 
		 admin.createTable(tableDescriptor);
		
		 tableDescriptor = new HTableDescriptor("reduce2");
		 tableDescriptor.addFamily(new HColumnDescriptor("xbar"));
		 tableDescriptor.addFamily(new HColumnDescriptor("stock"));
		 
		 admin.createTable(tableDescriptor);
		 
		 
		 Job job = new Job(config,"Read");
		 job.setJarByClass(Map1phase1.class);    // class that contains mapper
		 
		 Job job2 = new Job(config,"Read");
		 job2.setJarByClass(Map2phase2.class);
		 
		 Job job3 = new Job(config,"Read");
		 job3.setJarByClass(Map3phase3.class);
		 
		 
		 int NOfReducer1 = Integer.valueOf(2);	
		 job.setNumReduceTasks(NOfReducer1);
			
		 int NOfReducer2 = Integer.valueOf(2);
		 job2.setNumReduceTasks(NOfReducer2);
		 job3.setNumReduceTasks(1);
		 job3.setMapOutputKeyClass(Text.class);
		 
		 FileOutputFormat.setOutputPath(job3, new Path("Output"));
		 

		 Scan scan = new Scan();
		 scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		 scan.setCacheBlocks(false);  // don't set to true for MR jobs
		 // set other scan attrs

		 String sourceTable="raw";

		TableMapReduceUtil.initTableMapperJob(sourceTable,scan,Map1phase1.Mapper1.class,Text.class,Text.class,job);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		String targetTable="reduce1";
		TableMapReduceUtil.initTableReducerJob(targetTable,Map1phase1.Reducer1.class,job);
		
		 
		 
		 TableMapReduceUtil.initTableMapperJob(targetTable,scan,Map2phase2.Mapper2.class,Text.class,Text.class,job2);
		 String targetTable2="reduce2";
		 TableMapReduceUtil.initTableReducerJob(targetTable2,Map2phase2.Reducer2.class,job2);
				
		
		 TableMapReduceUtil.initTableMapperJob(targetTable2,scan,Map3phase3.Mapper3.class,Text.class,Text.class,job3);
		 job3.setReducerClass(Map3phase3.Reduce3.class);											

		System.out.println("\n**********Stock_Volatility-> Start**********\n");

		
		
		job.waitForCompletion(true);
		job2.waitForCompletion(true);

		boolean status = job3.waitForCompletion(true);
		if (status == true) {
			long end = new Date().getTime();
			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}
		System.out.println("\n**********Stock_volatility-> End**********\n");		
	}
}

