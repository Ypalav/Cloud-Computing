package com.cloud.hadoopimpl;

import java.io.*;
import java.nio.file.FileStore;
import org.apache.hadoop.fs.*;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.FileStatus;

public class TeraSort {
	private static final Log LOG = LogFactory.getLog(TeraSort.class);

	public static class SortMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		//provide relevant key and values to the map function
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String textKey = value.toString().substring(0, 10);
			String textValue = value.toString().substring(11, 98);
			output.collect(new Text(textKey), new Text(textValue));

		}
	}

	public static class SortReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}
		
		//Just iterate over the sorted values and store it in output file
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String value;
			while (values.hasNext()) {
				value = values.next().toString();
				output.collect(key, new Text(value));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		LOG.info("starting");
		JobConf conf = new JobConf(TeraSort.class);
		conf.setJobName("TeraSort");
		long start_t = System.currentTimeMillis();
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(SortMapper.class);
		conf.setCombinerClass(SortReduce.class);
		conf.setReducerClass(SortReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		long end_t = System.currentTimeMillis();
		
		LOG.info("Done - Total Time Spent -" + (end_t - start_t) / 1000 + " seconds");
		
		
	}

}
