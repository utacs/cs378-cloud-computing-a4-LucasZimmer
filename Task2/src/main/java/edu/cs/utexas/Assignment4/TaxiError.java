package edu.cs.utexas.Assignment4;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class TaxiError implements Comparable<TaxiError> {

        private final Text taxi;
        private final FloatWritable error;

        public TaxiError(Text taxi, FloatWritable error) {
            this.taxi = taxi;
            this.error = error;
        }

        public Text getTaxi() {
            return taxi;
        }

        public FloatWritable getError() {
            return error;
        }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(TaxiError other) {

            float diff = error.get() - other.error.get();
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){

            return "("+taxi.toString() +" , "+ error.toString()+")";
        }
    }

