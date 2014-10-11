package pairs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairsDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		String inputPath = "/input";
		String outputPath = "/output";
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(PairsDriver.class);
		job.setMapperClass(PairsMapper.class);
		job.setReducerClass(PairsReducer.class);
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setPartitionerClass(PairsPartitioner.class);
		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
