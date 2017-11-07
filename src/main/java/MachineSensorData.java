package main.java;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MachineSensorData{
    /**
     * Mapper class
     */
	public static class MachineSensorDataMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text sensorData = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = line.split("\\R+"); //Split end of line
			for (String w: data) {
				sensorData.set(w);
				context.write(sensorData, one);
			}
		}
	}

	/**
	 * Reducer class
	 */
	public static class MachineSensorDataReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable sumTotal = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			sumTotal.set(sum);
			context.write(key,  sumTotal);
		}
	}


	public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
		// setup job configuration
		Configuration conf = new Configuration();
		Job job = new Job(conf, "SensorDataCfg");

		// set mapper output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// set mapper and reducer classes
		job.setMapperClass(MachineSensorDataMap.class);
		job.setReducerClass(MachineSensorDataReduce.class);

		// set input and output file classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// load input and output files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

        System.out.println( "Done!" );
    }
}
