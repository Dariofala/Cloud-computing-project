package it.unipi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class MainDriver {

    // Comparatore per ordinare i voti in modo DECRESCENTE
    public static class DecreasingDoubleComparator extends WritableComparator {
        protected DecreasingDoubleComparator() {
            super(DoubleWritable.class, true);
        }

        @SuppressWarnings("rawtypes") // Questa riga dice a Java di ignorare i 2 warning
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DoubleWritable d1 = (DoubleWritable) a;
            DoubleWritable d2 = (DoubleWritable) b;
            return -1 * d1.compareTo(d2); // Il -1 inverte l'ordine
        }
    }

    // MAIN
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MainDriver <input path> <output path>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path intermediatePath1 = new Path(args[1] + "_tmp1"); 
        Path intermediatePath2 = new Path(args[1] + "_tmp2"); 
        Path finalOutputPath = new Path(args[1]);

        Configuration conf = new Configuration();


        // ---
        // Esecuzione Job 1
        // ---
        Job job1 = Job.getInstance(conf, "Job 1");
        job1.setJarByClass(MainDriver.class);
        
        // Collegamento classi interne Mapper1 e Reducer1
        job1.setMapperClass(Job1.Mapper1.class);
        job1.setReducerClass(Job1.Reducer1.class);

        // Numero di Reducer
        job1.setNumReduceTasks(4);
        
        // I tipi finali emessi dal Reducer1
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        // Input-Output Paths del Job 1
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, intermediatePath1);

        // Avvia il Job 1 e aspetta che finisca. Se fallisce, interrompe il programma.
        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 fallito. Interruzione.");
            System.exit(1);
        }


        // ---
        // Esecuzione Job 2
        // ---
        Job job2 = Job.getInstance(conf, "Job 2");
        job2.setJarByClass(MainDriver.class);
        
        // Diciamo ad Hadoop che il file in ingresso è un formato Chiave-Valore
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        
        job2.setMapperClass(Job2.Mapper2.class);
        job2.setCombinerClass(Job2.Combiner2.class);
        job2.setReducerClass(Job2.Reducer2.class);

        // Numero di Reducer
        job2.setNumReduceTasks(4);
        
        // I tipi emessi dal Mapper2 intermedi
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Job2.RatingCountWritable.class);
        
        // I tipi finali emessi dal Reducer2
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        // Input-Output Paths del Job 2
        FileInputFormat.addInputPath(job2, intermediatePath1);
        FileOutputFormat.setOutputPath(job2, intermediatePath2);

        // Avvia il Job 2 e aspetta che finisca. Se fallisce, interrompe il programma.
        if (!job2.waitForCompletion(true)) {
            System.err.println("Job 2 fallito. Interruzione.");
            System.exit(1);
        }


        // ---
        // Esecuzione Job 3
        // ---
        Job job3 = Job.getInstance(conf, "Job 3");
        job3.setJarByClass(MainDriver.class);

        // Diciamo ad Hadoop che il file in ingresso è in formato Chiave-Valore
        job3.setInputFormatClass(KeyValueTextInputFormat.class);

        job3.setMapperClass(Job3.Mapper3.class);
        job3.setReducerClass(Job3.Reducer3.class);

        // Assegnazione del comparatore per ordinare dal più alto al più basso
        job3.setSortComparatorClass(DecreasingDoubleComparator.class);

        // Per garantire un ordinamento globale, usiamo 1 Reducer
        job3.setNumReduceTasks(1);

        // I tipi emessi dal Mapper3 intermedi
        job3.setMapOutputKeyClass(DoubleWritable.class);
        job3.setMapOutputValueClass(Text.class);

        // I tipi finali emessi dal Reducer3
        job3.setOutputKeyClass(DoubleWritable.class);
        job3.setOutputValueClass(Text.class);

        // Input-Output Paths del Job 3
        FileInputFormat.addInputPath(job3, intermediatePath2);
        FileOutputFormat.setOutputPath(job3, finalOutputPath);

        // Avvia il Job 3 e aspetta che finisca. Se fallisce, interrompe il programma.
        if (!job3.waitForCompletion(true)) {
            System.err.println("Job 3 fallito. Interruzione.");
            System.exit(1);
        }
        System.exit(0);
    }
}