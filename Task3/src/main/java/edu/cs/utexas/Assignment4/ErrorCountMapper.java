package edu.cs.utexas.Assignment4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ErrorCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	//Setup
	public void setup(Context context) throws IOException, InterruptedException {
        for(int i = 0; i <24; i++) {
			context.write(new IntWritable(i), new IntWritable(0));
		}
    }

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		// StringTokenizer itr = new StringTokenizer(value.toString());
		// while (itr.hasMoreTokens()) {
		// 	word.set(itr.nextToken());
		// 	context.write(word, counter);
		// }

		String[] fields = value.toString().split(",");
		if(fields[6].isEmpty() || fields[7].isEmpty() || Float.parseFloat(fields[6]) == 0.0 || Float.parseFloat(fields[7]) == 0.0)
			try {
				int hour = Integer.parseInt(fields[2].substring(fields[2].indexOf(" ")+1, fields[2].indexOf(":")));
				context.write(new IntWritable(hour), counter);
			} catch (Exception e) {
				//Do nothing
			}
		if(fields[8].isEmpty() || fields[9].isEmpty() || Float.parseFloat(fields[8]) == 0 || Float.parseFloat(fields[9]) == 0)
			try {
				int hour = Integer.parseInt(fields[3].substring(fields[3].indexOf(" ")+1, fields[3].indexOf(":")));
				context.write(new IntWritable(hour), counter);
			} catch (Exception e) {
				//Do nothing
			}
	}
}