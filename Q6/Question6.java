package Q6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question6 {
    /*
     * mapper
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
    {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] record = value.toString().split("\t");
            String year = record[7];
            String case_status = record[1];
            context.write(new Text(year),new Text(case_status));
            
        }
    }
        
        /*
         * reducer
         */
    public static class MyReducer extends Reducer<Text, Text, Text, Text>
    {
    	double count=0.0;
        double certified=0.0;
        double certified_withdrawn=0.0;
        double withdrawn=0.0;
        double denied=0.0;
        double percertified=0.0;
        double percertified_withdrawn=0.0;
        double perwithdrawn=0.0;
        double perdenied=0.0;
        String case_status="";

        
       
    protected void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException
     {
       
       for(IntWritable val:values)
       {
           case_status=val.toString();
           count++;
           if(case_status.equals("CERTIFIED"))
           {
               certified++;
               
           }
           else if(case_status.equals("CERTIFIED-WITHDRAWN"))
           {
           certified_withdrawn++;
           }
           else if(case_status.equals("WITHDRAWN"))
           {
               withdrawn++;
           }
           else
           {
               denied++;
           }
        }
     
       
       
           
       percertified=(certified*100)/count;
       percertified_withdrawn=(certified_withdrawn*100)/count;
       perwithdrawn=(withdrawn*100)/count;
       perdenied=(denied*100)/count;
       
       String year=key.toString();
       String myValue=percertified+","+percertified_withdrawn+","+withdrawn+","+denied;
       
       
           
       context.write(new Text(year),new Text(myValue));
    }
}
public static void main(String[] args) throws Exception 
   {
        
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "percentage and count");
        
        job.setJarByClass(Question6.class);
        
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}   

    
	

