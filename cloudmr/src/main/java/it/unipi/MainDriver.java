package it.unipi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MainDriver <input path> <output path>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = new Configuration();

        // Inizializzazione Job 1
        Job job1 = Job.getInstance(conf, "Job 1: Test Generazione Coppie");
        job1.setJarByClass(MainDriver.class);
        
        // Collegamento classi interne Mapper1 e Reducer1
        job1.setMapperClass(Job1.Mapper1.class);
        job1.setReducerClass(Job1.Reducer1.class);

        // job1.setNumReduceTasks(3); VALUTARE NUMERO DI REDUCER
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        // Input-Output Paths
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        //job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);

        // Esecuzione
        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }
}