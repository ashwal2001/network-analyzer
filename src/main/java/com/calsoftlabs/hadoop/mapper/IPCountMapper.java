package com.calsoftlabs.hadoop.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.calsoftlabs.hadoop.bo.KeyValue;
import com.calsoftlabs.hadoop.util.StringUtils;

public class IPCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text keyMap = new Text();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String str = itr.nextToken();
			KeyValue keyValue = StringUtils.convertToken(str);
			if ((keyValue.getKey()).equals("src")) {
				keyMap.set(keyValue.getValue());
				context.write(keyMap, one);
			}
		}
	}

}
