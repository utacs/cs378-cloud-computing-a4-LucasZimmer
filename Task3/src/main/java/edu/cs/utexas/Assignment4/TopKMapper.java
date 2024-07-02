package edu.cs.utexas.Assignment4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


public class TopKMapper extends Mapper<Text, Text, Text, FloatWritable> {


	private PriorityQueue<DriverRate> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<DriverRate>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {


		float count = Float.parseFloat(value.toString());

		pq.add(new DriverRate(new Text(key), new FloatWritable(count)) );

		if (pq.size() > 10) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			DriverRate top = pq.poll();
			context.write(top.getDriver(), top.getRate());
		}
	}

}