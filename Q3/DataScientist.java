import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataScientistPositions {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			String job_title = record[4];
			String case_status = record[1];
			String soc_name = record[3];
			
			if(job_title.contains("DATA SCIENTIST") && case_status.equals("CERTIFIED"))
			{
				context.write(new Text(soc_name), one);
			}
			
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, NullWritable, Text>
	{
		TreeMap<LongWritable, Text> dataScientistJobs  = new TreeMap<LongWritable,Text>();
		
		long sum =0;
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException 
		{
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			
			String myValue = key.toString() +","+sum;
			
			dataScientistJobs.put(new LongWritable(sum), new Text(myValue));
			
			if(dataScientistJobs.size() > 5)
			{
				dataScientistJobs.remove(dataScientistJobs.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			for(Text t : dataScientistJobs.descendingMap().values())
			{
				context.write(NullWritable.get(), t);
			}
		}
		
		
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Most Number of Data Scientist Position");
		
		job.setJarByClass(DataScientistPositions.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
