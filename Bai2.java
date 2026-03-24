import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.util.*;

public class Bai2 {
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Map<String, String> movieGenresMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try (BufferedReader br = new BufferedReader(new FileReader("movies.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(", ");
                    if (parts.length >= 3) {
                        movieGenresMap.put(parts[0], parts[2]); 
                    }
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.isEmpty()) {
                String[] parts = line.split(", "); // UserID, MovieID, Rating, Timestamp
                if (parts.length >= 3) {
                    String movieId = parts[1];
                    double rating = Double.parseDouble(parts[2]);
                    String genresStr = movieGenresMap.get(movieId);

                    if (genresStr != null) {
                        String[] genres = genresStr.split("\\|"); 
                        for (String g : genres) {
                            context.write(new Text(g), new DoubleWritable(rating));
                        }
                    }
                }
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double avg = sum / count;
            context.write(new Text(key.toString() + ":"), 
                          new Text(String.format("%.2f", avg) + " (" + count + ")"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analysis");
        job.setJarByClass(Bai2.class);
        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}