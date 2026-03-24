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

public class Bai3 {

    public static class GenderMovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> userGenderMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Nạp users.txt vào bộ nhớ: UserID -> Gender
            try (BufferedReader br = new BufferedReader(new FileReader("users.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(", ");
                    if (parts.length >= 2) userGenderMap.put(parts[0], parts[1]);
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.isEmpty()) {
                String[] parts = line.split(", "); // UserID, MovieID, Rating, Timestamp
                if (parts.length >= 3) {
                    String userId = parts[0];
                    String movieId = parts[1];
                    String rating = parts[2];
                    String gender = userGenderMap.get(userId);

                    if (gender != null) {
                        context.write(new Text(movieId), new Text(gender + "_" + rating));
                    }
                }
            }
        }
    }

    public static class GenderMovieReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> movieNames = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try (BufferedReader br = new BufferedReader(new FileReader("movies.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(", ");
                    if (parts.length >= 2) movieNames.put(parts[0], parts[1]);
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumM = 0, sumF = 0;
            int countM = 0, countF = 0;

            for (Text val : values) {
                String[] data = val.toString().split("_");
                String gender = data[0];
                double rating = Double.parseDouble(data[1]);

                if (gender.equals("M")) {
                    sumM += rating;
                    countM++;
                } else if (gender.equals("F")) {
                    sumF += rating;
                    countF++;
                }
            }

            String movieTitle = movieNames.getOrDefault(key.toString(), "Unknown (ID: " + key + ")");
            String maleAvg = (countM > 0) ? String.format("%.2f", sumM / countM) : "N/A";
            String femaleAvg = (countF > 0) ? String.format("%.2f", sumF / countF) : "N/A";
            context.write(new Text(movieTitle + ":"), new Text(maleAvg + ", " + femaleAvg));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Gender Analysis");
        job.setJarByClass(Bai3.class);
        job.setMapperClass(GenderMovieMapper.class);
        job.setReducerClass(GenderMovieReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}