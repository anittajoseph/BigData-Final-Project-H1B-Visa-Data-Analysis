package h1b_trial1;



import java.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class DataEngineer {

	public static class MapClass extends Mapper<LongWritable,Text,NullWritable,Text>
	   {

	      public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	      {

	         try{

	            String[] record =value.toString().split("\t");
	            String job_title=record[4];
	            String year=record[7];
	            if(job_title.contains("DATA ENGINEER"))
	            {
	          	context.write(NullWritable.get(),new Text(year));
	            }
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }

 public static class ReduceClass extends Reducer<NullWritable,Text,NullWritable,Text>
  {

  Text result =new Text();
public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException
{
  long count1=0,count2=0,count3=0,count4=0,count5=0,count6=0;
  double cycle1=0,cycle2=0,cycle3=0,cycle4=0,cycle5=0;

  for (Text val : values)
		         {

		             String year=val.toString();
		             if(year.equals("2011"))
		             {
		            	 count1++;
		             }
		             else  if(year.equals("2012"))
		             {
		            	 count2++;
		             }
		             else  if(year.equals("2013"))
		             {
		            	 count3++;
		             }
		             else  if(year.equals("2014"))
		             {
		            	 count4++;
		             }
		             else  if(year.equals("2015"))
		             {
		            	 count5++;
		             }
		             else  if(year.equals("2016"))
		             {
		            	 count6++;
		             }
		         }

		         if(count1 !=0)
		           {
		        	   cycle1=((count2-count1)*100)/count1;
		           	}
		           else {
		        	   cycle1=0;
		           		}
		           if (count2 !=0)
		           {
		        	   cycle2=((count3-count2)*100)/count2;
		           }
		           else
		           {
		        	 cycle2=0;
		           }
		           if (count3 !=0)
		           {
		        	   cycle3=((count4-count3)*100)/count3;
		           }
		           else
		           {
		        	 cycle3=0;
		           }
		           if (count4 !=0)
		           {
		        	   cycle4=((count5-count4)*100)/count4;
		           }
		           else
		           {
		        	 cycle4=(count5-count4)*10;
		           }
		           if (count5 !=0)
		           {
		        	   cycle5=((count6-count5)*100)/count5;
		           }
		           else
		           {
		        	 cycle5=0;
		           }
	double avg=(cycle1+cycle2+cycle3+cycle4+cycle5)/5;
	String newavg=String.format("%.2f", avg);

 String myrow=cycle1+","+cycle2+","+cycle3+","+","+cycle4+","+cycle5+","+newavg;
result.set(myrow);

	context.write(key, result);

		    }
}


	  public static void main(String[] args) throws Exception
	  {
		    Configuration conf = new Configuration();
		   Job job = Job.getInstance(conf);
		    conf.set("mapreduce.output.textoutputformat.separator", ",");
		    job.setJarByClass(DataEngineer.class);
		    job.setMapperClass(MapClass.class);
		     job.setReducerClass(ReduceClass.class);
    job.setMapOutputKeyClass(NullWritable.class);
		    job.setMapOutputValueClass(Text.class);

		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);

		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	  }
	   }
