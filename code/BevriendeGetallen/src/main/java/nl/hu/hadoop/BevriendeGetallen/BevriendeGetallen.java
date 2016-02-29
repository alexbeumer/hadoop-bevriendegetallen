package main.java.nl.hu.hadoop.BevriendeGetallen;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class BevriendeGetallen {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(BevriendeGetallen.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(BevriendeGetallenMapper.class);
		job.setCombinerClass(BevriendeGetallenReducer.class);
		job.setReducerClass(BevriendeGetallenReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}

class BevriendeGetallenMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		int myNumber = Integer.parseInt(value.toString());
		int sum = 0;
		for(int i = 1; i <= myNumber / 2; i++) {
			if (myNumber % i == 0) {
				sum += i;
			}
		}
		sum += myNumber;
		context.write(new IntWritable(sum), new IntWritable(myNumber));
	}
}

class BevriendeGetallenReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		Set<Integer> outerLoop = new HashSet<Integer>();
		Set<Integer> innerLoop = new HashSet<Integer>();
		Iterator<IntWritable> valuesLoop = values.iterator();
		while(valuesLoop.hasNext()) {
			IntWritable value = valuesLoop.next();
			outerLoop.add(value.get());
			innerLoop.add(value.get());
		}

		for(int outerValue : outerLoop) {
			int expectedInnerValue = key.get() - outerValue;

			for(int innerValue : innerLoop) {
				if(expectedInnerValue == innerValue && outerValue != innerValue) {
					context.write(new IntWritable(outerValue), new IntWritable(innerValue));
				}
			}
		}
	}
}