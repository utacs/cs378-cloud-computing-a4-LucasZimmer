package edu.cs.utexas.Assignment4;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class DriverRate implements Comparable<DriverRate> {

        private final Text driver;
        private final FloatWritable rate;

        public DriverRate(Text driver, FloatWritable rate) {
            this.driver = driver;
            this.rate = rate;
        }

        public Text getDriver() {
            return driver;
        }

        public FloatWritable getRate() {
            return rate;
        }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(DriverRate other) {

            float diff = rate.get() - other.rate.get();
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){

            return "("+driver.toString() +" , "+ rate.toString()+")";
        }
    }

