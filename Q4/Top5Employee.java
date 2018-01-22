package top5;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Top5Employee{

	/*
	 * Mapper class
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			
			String employer_name = record[2];
			String year = record[7];
			
			String myVal = year +","+1;
			
			context.write(new Text(employer_name), new Text(myVal));
		}
		
	}
	
	/*
	 * Partitioner class
	 */
	
	public static class YearPartitioner extends Partitioner<Text, Text>
	{
		@Override
		public int getPartition(Text key, Text value, int numReduceTask) {
			
			String[] token = value.toString().split(",");
			
			String year = token[0];
			
			if(year.equals("2011"))
			{
				return 0 % numReduceTask;
			}
			else if(year.equals("2012"))
			{
				return 1 % numReduceTask;
			}
			else if(year.equals("2013"))
			{
				return 2 % numReduceTask; 
			}
			else if(year.equals("2014"))
			{
				return 3 % numReduceTask;
			}
			else if(year.equals("2015"))
			{
				return 4 % numReduceTask;
			}
			else
			{
				return 5 % numReduceTask;
			}
		}

	}
	
	/*
	 * Reducer class
	 */
	public static class MyReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		TreeMap<Integer, Text> top5 = new TreeMap<Integer, Text>();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			int count = 0;
			int sum = 0;
			String year ="";
			for(Text val : values)
			{
				String[] str = val.toString().split(",");
				year = str[0];
				count = Integer.parseInt(str[1]);
				
				sum = sum + count;
			}
			
			String employer_name = key.toString();
			String myValue = year+ ","+employer_name+","+String.format("%d", sum);
			
			top5.put(new Integer(sum), new Text(myValue));
			if(top5.size() > 5)
			{
				top5.remove(top5.firstKey());
			}
		}
		
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException {
			
			for(Text t : top5.descendingMap().values())
			{
				context.write(NullWritable.get(), t);
			}
		}
	}
	
	/*
	 * Driver code or Configuration class
	 */
     public static void main(String[] args) throws Exception {
		
    	 Configuration conf  =  new Configuration();
    	 
    	 Job job = Job.getInstance(conf, "Top 5  Employer names");
    	 
    	 job.setJarByClass(Top5Employee.class);
    	 
    	 job.setMapperClass(MyMapper.class);
    	 
    	 job.setPartitionerClass(YearPartitioner.class);
    	 job.setNumReduceTasks(6);
    	 
    	 job.setReducerClass(MyReducer.class);
    	 
    	 job.setMapOutputKeyClass(Text.class);
    	 job.setMapOutputValueClass(Text.class);
    	 
    	 job.setOutputKeyClass(NullWritable.class);
    	 job.setOutputValueClass(Text.class);
    	 
    	 FileInputFormat.addInputPath(job, new Path(args[0]));
    	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	 
    	 System.exit(job.waitForCompletion(true) ? 0 : 1);
    	
	}
	
}

