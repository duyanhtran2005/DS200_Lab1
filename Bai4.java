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

public class Bai4 {

    public static class AgeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, Integer> userAgeMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader("users.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(", ");
                    if (parts.length >= 3) {
                        userAgeMap.put(parts[0], Integer.parseInt(parts[2]));
                    }
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.isEmpty()) {
                String[] parts = line.split(", ");
                if (parts.length >= 3) {
                    String userId = parts[0];
                    String movieId = parts[1];
                    String rating = parts[2];
                    Integer age = userAgeMap.get(userId);

                    if (age != null) {
                        String group = "";
                        if (age < 18) group = "0-18";
                        else if (age <= 35) group = "18-35";
                        else if (age <= 50) group = "35-50";
                        else group = "50+";
                        context.write(new Text(movieId), new Text(group + "_" + rating));
                    }
                }
            }
        }
    }

    public static class AgeReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> movieNames = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
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
            Map<String, Double> sums = new HashMap<>();
            Map<String, Integer> counts = new HashMap<>();
            String[] groups = {"0-18", "18-35", "35-50", "50+"};

            for (String g : groups) {
                sums.put(g, 0.0);
                counts.put(g, 0);
            }

            for (Text val : values) {
                String[] data = val.toString().split("_");
                String group = data[0];
                double rating = Double.parseDouble(data[1]);
                sums.put(group, sums.get(group) + rating);
                counts.put(group, counts.get(group) + 1);
            }

            StringBuilder result = new StringBuilder("[");
            for (int i = 0; i < groups.length; i++) {
                String g = groups[i];
                String avg = (counts.get(g) > 0) ? String.format("%.2f", sums.get(g) / counts.get(g)) : "N/A";
                result.append(g).append(": ").append(avg);
                if (i < groups.length - 1) result.append(", ");
            }
            result.append("]");

            String title = movieNames.getOrDefault(key.toString(), "Unknown");
            context.write(new Text(title + ":"), new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Analysis");
        job.setJarByClass(Bai4.class);
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}