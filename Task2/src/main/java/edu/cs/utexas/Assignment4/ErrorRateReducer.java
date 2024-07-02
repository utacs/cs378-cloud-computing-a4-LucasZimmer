package edu.cs.utexas.Assignment4;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ErrorRateReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

   public void reduce(Text key, Iterable<FloatWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       float sum = 0;
       float total = 0;
       
       for (FloatWritable value : values) {
           sum += value.get();
           total += 1;
       }
       
       context.write(key, new FloatWritable(sum/total));
   }
}