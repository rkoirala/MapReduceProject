package com.algorithm.pairs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class PairsPartitioner implements Partitioner<Pair, DoubleWritable> {

	@Override
	public int getPartition(Pair key, DoubleWritable value, int numReduceTasks) {

		return (int) key.Firstword % numReduceTasks;
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}
}
