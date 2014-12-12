package com.algorithm.stripes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class StripesPartitioner implements
		Partitioner<LongWritable, MapWritable> {

	@Override
	public int getPartition(LongWritable key, MapWritable value,
			int numReduceTasks) {

		return (int) key.get() % numReduceTasks;
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}
}
