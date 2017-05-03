package h1b_trial2;


import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class DataEngineerUS extends Configured implements Tool
{

	public static class MapperClass extends Mapper<LongWritable,Text,Text,Text>
	{
	
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		try
			{
			

					String record[] =value.toString().split("\t");
					String job_title=record[4];
String year=record[7];
					String worksite=record[8];
					String data=year;
				
					
					if(job_title.contains("DATA ENGINEER"))
					{
												context.write(new Text(worksite),new Text(data));
					}

			}
			catch(Exception e)
			{
			System.out.println(e.getMessage());
			}
		}
	}



	public static class ReducerClass extends Reducer<Text,Text,NullWritable,Text>
	{
	
		
		TreeMap<Long,Text> topMap=new TreeMap<Long,Text>();
		public void reduce(Text key,Iterable<Text> values,Context context)
		{
			
			String mykey=key.toString();
String year=null;
			long total_petition = 0;
			for(Text val:values)
			{
				total_petition++;
				year = val.toString();
				
			}
			String petitions=String.format("%d",total_petition);
			String myvalue=mykey+"\t"+year+"\t"+petitions;
			topMap.put(new Long(total_petition), new Text(myvalue));

		}
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			for(Text t:topMap.descendingMap().values()) 
			{
				context.write(NullWritable.get(),new Text(t));
			}
		}


	}

	public static class MyPartition extends Partitioner<Text,Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks)
		{
						
			String year=value.toString();
			if(year.equals("2011"))
			{
			return 0 %numReduceTasks;
			}
			else if(year.equals("2012"))
			{
			return 1 %numReduceTasks;
			}
			else if(year.equals("2013"))
			{
			return 2 %numReduceTasks;
			}
			else if(year.equals("2014"))
			{
			return 3 %numReduceTasks;
			}
			else if(year.equals("2015"))
			{
			return 4 %numReduceTasks;
			}
			else if(year.equals("2016"))
			{
			return 5 %numReduceTasks;
			}
			else
			{
				return 6;
			}
		}
	}
		@Override
		public int run(String[] arg) throws Exception 
		{
	               Configuration conf = new Configuration();
	                Job job = Job.getInstance(conf);
		    job.setJarByClass(DataEngineerUS.class);
		    job.setJobName("Find which part of US has most data engineer jobs ");
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setMapperClass(MapperClass.class);
		    job.setReducerClass(ReducerClass.class);
		    job.setPartitionerClass(MyPartition.class);
		   job.setNumReduceTasks(7);
		    
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    
		    job.setOutputKeyClass(NullWritable.class);
		   job.setOutputValueClass(Text.class);
		    
		    
		    FileInputFormat.addInputPath(job, new Path(arg[0]));
		    FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    return 0;		
		}
		public static void main(String args[]) throws Exception
		{
		ToolRunner.run(new Configuration(),new DataEngineerUS (), args);
		System.exit(0);
			
		}
	}

