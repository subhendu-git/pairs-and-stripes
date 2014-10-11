package stripes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StripesDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		String inputPath = "/input";
		String outputPath = "/output";
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(StripesDriver.class);
		job.setMapperClass(StripesMapper.class);
		job.setCombinerClass(StripesCombiner.class);
		job.setReducerClass(StripesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
