package edu.cs.utexas.Assignment4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EarningsReducer extends  Reducer<Text, Text, Text, Text> {

   public void reduce(Text key, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
       float total = 0;
       float seconds = 0;
       
       for (Text value : values) {
            String[] v = value.toString().split(" ");
            total += Float.parseFloat(v[0]);
            seconds += Float.parseFloat(v[1]);
       }
       
       context.write(key, new Text(Float.toString(60.0f*total/seconds)));
   }
}