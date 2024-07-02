package edu.cs.utexas.Assignment4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;



public class TopKReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

    private PriorityQueue<DriverRate> pq;



   public void setup(Context context) {

       pq = new PriorityQueue<DriverRate>();
   }


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
   public void reduce(Text key, Iterable<FloatWritable> values, Context context)
           throws IOException, InterruptedException {

       for (FloatWritable value : values)
           pq.add(new DriverRate(new Text(key), new FloatWritable(value.get())));

       while (pq.size() > 10) {
           pq.poll();
       }
   }


    public void cleanup(Context context) throws IOException, InterruptedException {

        ArrayList<DriverRate> values = new ArrayList<DriverRate>(10);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }


        // reverse so they are ordered in descending order
        Collections.reverse(values);


        for (DriverRate value : values)
            context.write(value.getDriver(), value.getRate());
    }
}