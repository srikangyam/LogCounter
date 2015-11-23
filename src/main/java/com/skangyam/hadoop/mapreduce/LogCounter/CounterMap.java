package com.skangyam.hadoop.mapreduce.LogCounter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
	          throws IOException,InterruptedException{
		if (value.toString().startsWith("INFO:")){
			context.getCounter(DEBUG_COUNTER.INFO).increment(1);
			context.write(new Text("INFO:"), new IntWritable(1));
		} else if (value.toString().startsWith("WARNING:")) {
			context.getCounter(DEBUG_COUNTER.WARNING).increment(1);
			context.write(new Text("WARNING"), new IntWritable(1));
		} else if (value.toString().startsWith("ERROR:")) {
			context.getCounter(DEBUG_COUNTER.ERROR).increment(1);
			context.write(new Text("ERROR"), new IntWritable(1));
		}
		
	}

}
