package edu.cs.utexas.Assignment4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EarningsMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");
		if(fields.length == 17 && fields[1].matches("^[0-F]{32}") && fields[4].matches("[0-9]*") && Float.parseFloat(fields[4]) > 0 && fields[16].matches("[0-9]+\\.[0-9]{2}")) {
			String text = fields[16] + " " + fields[4];
			context.write(new Text(fields[1]), new Text(text));
		}
	}
}