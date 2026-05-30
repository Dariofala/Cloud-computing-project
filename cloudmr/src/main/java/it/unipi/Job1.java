package it.unipi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

public class Job1 {

    // 1 MAPPER
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");
            if (columns.length < 5) return;

            String idFilm = columns[0].trim();
            String nomePersona = columns[1].trim();
            String ruolo = columns[2].trim();
            String votoStr = columns[3].trim();
            String numVotiStr = columns[4].trim();

            if (idFilm.isEmpty() || idFilm.equals("NaN") || idFilm.equals("\\N")) return;
            if (nomePersona.isEmpty() || nomePersona.equals("NaN")|| nomePersona.equals("\\N")) return;
            if (ruolo.isEmpty() || ruolo.equals("NaN")|| ruolo.equals("\\N")) return;
            if (votoStr.isEmpty() || votoStr.equals("NaN")|| votoStr.equals("\\N")) return;
            if (numVotiStr.isEmpty() || numVotiStr.equals("NaN")|| numVotiStr.equals("\\N")) return;

            // Filtro per numero di voti
            try {
                int numVoti = (int) Double.parseDouble(numVotiStr);
                if (numVoti < 100000){
                    return; 
                }
            } catch (NumberFormatException e) {
                return; 
            }
            

            if (ruolo.equals("actor") || ruolo.equals("actress") || ruolo.equals("director")) {
                outKey.set(idFilm);
                outValue.set(ruolo + ";;;" + nomePersona + ";;;" + votoStr);
                context.write(outKey, outValue);
            }
        }
    }

    // 2 PARTITIONER - SHUFFLE & SORT

    // 3 REDUCER
    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> directors = new ArrayList<>();
            ArrayList<String> actors = new ArrayList<>();
            String votoFilm = "";

            for (Text val : values) {
                String[] parts = val.toString().split(";;;");
                String ruolo = parts[0];
                String nome = parts[1];
                votoFilm = parts[2];
                if (ruolo.equals("director")) {
                    directors.add(nome);
                } else {
                    actors.add(nome);
                }
            }

            if(directors.size() > 50 || actors.size() > 500){
                return; // Skip di titoli con troppi collaboratori per non bloccare il reducer
            }

            // Prodotto Cartesiano
            for (String director : directors) {
                for (String actor : actors) {
                    if (director.equals(actor)) continue;
                    outKey.set(director + "_" + actor);
                    outValue.set(votoFilm);
                    context.write(outKey, outValue);
                }
            }
        }
    }
}
