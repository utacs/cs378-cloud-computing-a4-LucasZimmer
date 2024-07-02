package edu.cs.utexas.Assignment4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


public class TopKMapper extends Mapper<Text, Text, Text, FloatWritable> {


	private PriorityQueue<TaxiError> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<TaxiError>();

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

		pq.add(new TaxiError(new Text(key), new FloatWritable(count)) );

		if (pq.size() > 5) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			TaxiError top = pq.poll();
			context.write(top.getTaxi(), top.getError());
		}
	}

}