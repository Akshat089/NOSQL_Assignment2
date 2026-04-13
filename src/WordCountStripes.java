import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountStripes {

    public static class StripesMapper extends Mapper<Object, Text, Text, MapWritable> {
        private MapWritable stripe = new MapWritable();
        private Text wordKey = new Text();
        private Set<String> top50 = new HashSet<>();
        private int d;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            d = context.getConfiguration().getInt("neighbor.distance", 1);
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(
                    new FileReader(new Path(cacheFiles[0].getPath()).getName())
                );
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length > 0 && !parts[0].trim().isEmpty()) {
                        top50.add(parts[0].trim().toLowerCase());
                    }
                }
                reader.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] rawTokens = value.toString().toLowerCase().split("[^a-z]+");

            // Keep ALL real words for correct distance counting
            List<String> tokens = new ArrayList<>();
            for (String t : rawTokens) {
                if (!t.isEmpty()) tokens.add(t);
            }

            for (int i = 0; i < tokens.size(); i++) {
                String word = tokens.get(i);
                if (!top50.contains(word)) continue;

                stripe.clear();
                wordKey.set(word);

                // Look ahead up to distance d (actual word positions)
                for (int j = i + 1; j < Math.min(i + d + 1, tokens.size()); j++) {
                    String neighbor = tokens.get(j);
                    if (!top50.contains(neighbor) || word.equals(neighbor)) continue;

                    Text neighborText = new Text(neighbor);
                    if (stripe.containsKey(neighborText)) {
                        IntWritable count = (IntWritable) stripe.get(neighborText);
                        count.set(count.get() + 1);
                    } else {
                        stripe.put(neighborText, new IntWritable(1));
                    }
                }

                if (!stripe.isEmpty()) {
                    context.write(wordKey, stripe);
                }
            }
        }
    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            MapWritable resultStripe = new MapWritable();

            for (MapWritable stripe : values) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text neighbor = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();

                    if (resultStripe.containsKey(neighbor)) {
                        IntWritable total = (IntWritable) resultStripe.get(neighbor);
                        total.set(total.get() + count.get());
                    } else {
                        resultStripe.put(neighbor, new IntWritable(count.get()));
                    }
                }
            }
            context.write(key, new Text(resultStripe.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: WordCountStripes <input> <output> <distance> <top50file>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("neighbor.distance", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "stripes d=" + args[2]);
        job.setJarByClass(WordCountStripes.class);
        job.setMapperClass(StripesMapper.class);
        job.setReducerClass(StripesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(new Path(args[3]).toUri());

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        if (success) {
            System.out.println("=========================================");
            System.out.println("JOB SUCCESSFUL");
            System.out.println("DISTANCE (d): " + args[2]);
            System.out.println("RUNTIME: " + (endTime - startTime) / 1000.0 + " seconds");
            System.out.println("=========================================");
        }
        System.exit(success ? 0 : 1);
    }
}