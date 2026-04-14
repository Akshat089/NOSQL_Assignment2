import java.io.*;
import java.net.URI;
import java.util.*;
import opennlp.tools.stemmer.PorterStemmer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDFJob {

    public static class TFMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        private Set<String> stopwords = new HashSet<>();
        private Set<String> topTerms = new HashSet<>();
        private PorterStemmer stemmer = new PorterStemmer();

        private String currentDocID = "";
        private MapWritable stripe = new MapWritable();

        @Override
        protected void setup(Context context) throws IOException {
            // Load cache files using symlinks
            loadCacheFile("stopwords.txt", stopwords);
            loadCacheFile("top100.txt", topTerms);
        }

        private void loadCacheFile(String fileName, Set<String> targetSet) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length > 0) targetSet.add(parts[0].trim().toLowerCase());
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Detect new document boundaries in a combined file
            if (line.startsWith("###DOC###")) {
                // Emit the completed stripe for the PREVIOUS document
                if (!currentDocID.isEmpty() && !stripe.isEmpty()) {
                    context.write(new Text(currentDocID), new MapWritable(stripe));
                    stripe.clear();
                }
                // Update to new DocID
                String[] parts = line.split("\\s+");
                currentDocID = (parts.length > 1) ? parts[1] : "unknown";
                return;
            }

            if (currentDocID.isEmpty()) return;

            // Process text content
            String cleanLine = line.toLowerCase().replaceAll("[^a-z\\s]", " ");
            StringTokenizer itr = new StringTokenizer(cleanLine);

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (stopwords.contains(token) || token.length() < 2) continue;

                String stemmed = stemmer.stem(token);

                if (topTerms.contains(stemmed)) {
                    Text termText = new Text(stemmed);
                    IntWritable count = (IntWritable) stripe.get(termText);
                    if (count == null) {
                        stripe.put(termText, new IntWritable(1));
                    } else {
                        count.set(count.get() + 1);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit the very last document's stripe
            if (!currentDocID.isEmpty() && !stripe.isEmpty()) {
                context.write(new Text(currentDocID), stripe);
            }
        }
    }

    // Reducer remains the same as your provided version
    public static class TFReducer extends Reducer<Text, MapWritable, Text, Text> {
        private Map<String, Integer> dfMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader("top100.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) dfMap.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> finalCounts = new HashMap<>();
            for (MapWritable stripe : values) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    String term = entry.getKey().toString();
                    int count = ((IntWritable) entry.getValue()).get();
                    finalCounts.put(term, finalCounts.getOrDefault(term, 0) + count);
                }
            }

            for (Map.Entry<String, Integer> entry : finalCounts.entrySet()) {
                String term = entry.getKey();
                int tf = entry.getValue();
                int df = dfMap.getOrDefault(term, 1);
                double score = tf * Math.log(10000.0 /df + 1);
                context.write(key, new Text(term + "\t" + score));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf, "TF-IDF Combined Stripes");
        job.setJarByClass(TFIDFJob.class);
        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add symlinks using the '#' syntax
        job.addCacheFile(new URI("file:///mnt/c/Users/dell/IdeaProjects/hadoop/src/stopwords.txt#stopwords.txt"));
        job.addCacheFile(new URI("file:///mnt/c/Users/dell/IdeaProjects/hadoop/src/top100.txt#top100.txt"));
        job.addCacheFile(new URI("file:///mnt/c/Users/dell/IdeaProjects/hadoop/src/df.txt#df.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        if (success) {
            System.out.println("Runtime: " + (endTime - startTime) / 1000.0 + " seconds");
        }
        System.exit(success ? 0 : 1);
    }
}