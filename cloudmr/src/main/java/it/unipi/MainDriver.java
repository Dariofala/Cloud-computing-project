package it.unipi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class MainDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MainDriver <input path> <output path>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path intermediatePath = new Path(args[1] + "_tmp1"); 
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
        FileOutputFormat.setOutputPath(job1, intermediatePath);

        // Avvia il Job 1 e aspetta che finisca. Se fallisce, interrompe il programma.
        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 fallito. Interruzione.");
            System.exit(1);
        }


        // ---
        // Esecuzione Job 2
        // ---
        Job job2 = Job.getInstance(conf, "Job 2: Aggregazione Medie");
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
        FileInputFormat.addInputPath(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        // Avvia il Job 2 e aspetta che finisca. Se fallisce, interrompe il programma.
        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 2 fallito. Interruzione.");
            System.exit(1);
        }
        System.exit(0);
    }
}