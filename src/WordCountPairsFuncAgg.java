import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountPairsFuncAgg {

    public static class PairsFuncAggMapper extends Mapper<Object, Text, Text, IntWritable> {
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
                    if (parts.length > 0 && !parts[0].trim().isEmpty())
                        top50.add(parts[0].trim().toLowerCase());
                }
                reader.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] rawTokens = value.toString().toLowerCase().split("[^a-z]+");
            List<String> tokens = new ArrayList<>();
            for (String t : rawTokens) if (!t.isEmpty()) tokens.add(t);

            // Local HashMap per map() call (one line/record at a time)
            Map<String, Integer> localCounts = new HashMap<>();

            for (int i = 0; i < tokens.size(); i++) {
                String word1 = tokens.get(i);
                if (!top50.contains(word1)) continue;
                for (int j = i + 1; j < Math.min(i + d + 1, tokens.size()); j++) {
                    String word2 = tokens.get(j);
                    if (!top50.contains(word2) || word1.equals(word2)) continue;
                    String joint = (word1.compareTo(word2) < 0)
                        ? word1 + "," + word2 : word2 + "," + word1;
                    localCounts.merge(joint, 1, Integer::sum);
                }
            }

            // Emit aggregated counts for this single line
            Text pair = new Text();
            for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
                pair.set(entry.getKey());
                context.write(pair, new IntWritable(entry.getValue()));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: WordCountPairsFuncAgg <input> <output> <distance> <top50file>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("neighbor.distance", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "pairs func-level agg d=" + args[2]);
        job.setJarByClass(WordCountPairsFuncAgg.class);
        job.setMapperClass(PairsFuncAggMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(new Path(args[3]).toUri());

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        if (success) {
            System.out.println("=========================================");
            System.out.println("PAIRS FUNC-LEVEL AGG - JOB SUCCESSFUL");
            System.out.println("DISTANCE (d): " + args[2]);
            System.out.println("RUNTIME: " + (endTime - startTime) / 1000.0 + " seconds");
            System.out.println("=========================================");
        }
        System.exit(success ? 0 : 1);
    }
}