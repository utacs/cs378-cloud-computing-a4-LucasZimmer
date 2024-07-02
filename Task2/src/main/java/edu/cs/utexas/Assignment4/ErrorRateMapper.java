package edu.cs.utexas.Assignment4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ErrorRateMapper extends Mapper<Object, Text, Text, FloatWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");
		Text taxi = new Text(fields[0]);
		if(fields[6].isEmpty() || fields[7].isEmpty() || Float.parseFloat(fields[6]) == 0.0 || Float.parseFloat(fields[7]) == 0.0 || fields[8].isEmpty() || fields[9].isEmpty() || Float.parseFloat(fields[8]) == 0 || Float.parseFloat(fields[9]) == 0)
			context.write(taxi, new FloatWritable(1));
		else
			context.write(taxi, new FloatWritable(0));
	}
}