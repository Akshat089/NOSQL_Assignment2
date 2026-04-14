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

public class DFJob {

    public static class DFMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text term = new Text();
        private Text docID = new Text();

        private Set<String> stopwords = new HashSet<>();
        private PorterStemmer stemmer = new PorterStemmer();

        // 🔥 track current document
        private String currentDoc = "";

        @Override
        protected void setup(Context context) throws IOException {

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"));
                String line;
                while ((line = br.readLine()) != null) {
                    stopwords.add(line.trim().toLowerCase());
                }
                br.close();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 🔥 detect new document
            if (line.startsWith("###DOC###")) {
                String[] parts = line.split(" ");
                if (parts.length > 1) {
                    currentDoc = parts[1];
                }
                return;
            }

            if (currentDoc.isEmpty()) return;

            docID.set(currentDoc);

            line = line.toLowerCase().replaceAll("[^a-z\\s]", " ");
            StringTokenizer itr = new StringTokenizer(line);

            Set<String> uniqueTerms = new HashSet<>();

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().trim();

                if (!token.matches("[a-z]+")) continue;
                if (stopwords.contains(token)) continue;

                String stemmed = stemmer.stem(token);

                if (stemmed.length() > 1) {
                    uniqueTerms.add(stemmed);
                }
            }

            for (String t : uniqueTerms) {
                term.set(t);
                context.write(term, docID);
            }
        }
    }

    public static class DFReducer extends Reducer<Text, Text, Text, IntWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> uniqueDocs = new HashSet<>();

            for (Text val : values) {
                uniqueDocs.add(val.toString());
            }

            context.write(key, new IntWritable(uniqueDocs.size()));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: DFJob <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // ✅ LOCAL MODE (correct now)
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf, "Document Frequency (Combined)");
        job.setJarByClass(DFJob.class);

        job.setMapperClass(DFMapper.class);
        job.setReducerClass(DFReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // ✅ cache file
        job.addCacheFile(new URI("file:///mnt/c/Users/dell/IdeaProjects/hadoop/src/stopwords.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // ⏱ runtime tracking
        long start = System.currentTimeMillis();

        boolean success = job.waitForCompletion(true);

        long end = System.currentTimeMillis();

        if (success) {
            System.out.println("=================================");
            System.out.println("Job Completed");
            System.out.println("Time: " + (end - start) / 1000.0 + " sec");
            System.out.println("=================================");
            System.exit(0);
        } else {
            System.err.println("Job Failed!");
            System.exit(1);
        }
    }
}