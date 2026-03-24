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

public class Bai1 {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.isEmpty()) {
                String[] parts = line.split(", ");
                if (parts.length >= 3) {
                    context.write(new Text(parts[1]), new DoubleWritable(Double.parseDouble(parts[2])));
                }
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Map<String, String> movieNames = new HashMap<>();
        private String maxMovie = "";
        private double maxRating = -1.0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Nạp movies.txt vào Map [cite: 1]
            try (BufferedReader br = new BufferedReader(new FileReader("movies.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(", ");
                    if (parts.length >= 2) movieNames.put(parts[0], parts[1]);
                }
            } catch (Exception e) {}
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double avg = sum / count;
            String movieTitle = movieNames.getOrDefault(key.toString(), "Unknown");

            // Format Output: MovieTitle AverageRating: xx (TotalRatings: xx)
            context.write(new Text(movieTitle), new Text("AverageRating: " + String.format("%.1f", avg) + " (TotalRatings: " + count + ")"));

            // Tìm phim cao nhất (tối thiểu 5 lượt đánh giá)
            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = movieTitle;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                context.write(new Text("\n" + maxMovie), new Text("is the highest rated movie with an average rating of " + String.format("%.1f", maxRating) + " among movies with at least 5 ratings."));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Analysis");
        job.setJarByClass(Bai1.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}