package it.unipi;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Job3 {

    // 1 MAPPER
    public static class Mapper3 extends Mapper<Text, Text, DoubleWritable, Text> {
        private DoubleWritable outKey = new DoubleWritable();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            try {
                double media = Double.parseDouble(value.toString());
                outKey.set(media);
                
                // Invertiamo Chiave e Valore.
                context.write(outKey, key);
                
            } catch (NumberFormatException e) {
                // Gestione di sicurezza per ignorare eventuali righe vuote o malformate
                System.err.println("Errore nel parsing del voto nel Job 3: " + value.toString());
            }
        }
    }

    // 2 REDUCER 
    public static class Reducer3 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Se più coppie hanno esattamente la stessa media il framework le raggrupperà nello stesso 'Iterable'.
            // Noi le stampiamo una per una.
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }
}