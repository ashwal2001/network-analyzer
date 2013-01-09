package com.calsoftlabs.hadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.calsoftlabs.hadoop.mapper.IPCountMapper;
import com.calsoftlabs.hadoop.reducer.IPCountReducer;

public class IPCountDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: IPCountDriver <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "IPCountDriver");
		job.setJarByClass(IPCountDriver.class);

		job.setMapperClass(IPCountMapper.class);
		job.setCombinerClass(IPCountReducer.class);
		job.setReducerClass(IPCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		// TextOutputFormat.setCompressOutput(job, true);
		// TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		// TextOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
