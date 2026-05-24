package it.unipi;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Job2 {

    // WRITABLE 
    public static class RatingCountWritable implements Writable {
        private double sumOfRatings;
        private int count;

        public RatingCountWritable() {}
        public RatingCountWritable(double sum, int count) {
            this.sumOfRatings = sum;
            this.count = count;
        }
        public double getSumOfRatings() { return sumOfRatings; }
        public int getCount() { return count; }

        @Override
        public void write(DataOutput out) throws IOException { // Chiamata da Hadoop per serializzazione
            out.writeDouble(sumOfRatings);
            out.writeInt(count);
        }
        @Override
        public void readFields(DataInput in) throws IOException { // Chiamata da Hadoop per deserializzazione
            sumOfRatings = in.readDouble();
            count = in.readInt();
        }
    }

    // 1 MAPPER
    public static class Mapper2 extends Mapper<Text, Text, Text, RatingCountWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            double voto = Double.parseDouble(value.toString());
            context.write(key, new RatingCountWritable(voto, 1)); // Invio dati al buffer di Hadoop
        }
    }

    // 2 COMBINER --> Aggregazione dati post Mapper
    public static class Combiner2 extends Reducer<Text, RatingCountWritable, Text, RatingCountWritable> {
        @Override
        protected void reduce(Text key, Iterable<RatingCountWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (RatingCountWritable val : values) {
                sum += val.getSumOfRatings();
                count += val.getCount();
            }
            context.write(key, new RatingCountWritable(sum, count));
        }
    }

    // 3 PARTITIONER - SHUFFLE & SORT

    // 4 REDUCER
    public static class Reducer2 extends Reducer<Text, RatingCountWritable, Text, Text> {
        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<RatingCountWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            // Aggregazione dati post SHUFFLE & SORT
            for (RatingCountWritable val : values) {
                sum += val.getSumOfRatings();
                count += val.getCount();
            }

            // Scarta le coppie con meno di 10 collaborazioni
            if (count >= 10) {
                double media = sum / count;
                outValue.set(String.valueOf(media));
                context.write(key, outValue);
            }
        }
    }
}
