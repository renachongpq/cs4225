// Name: Rena Chong
// TopkCommonWords.java

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.PriorityQueue;
import java.util.Comparator;

// import data types
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

    // Mapper Class takes in 4 arguments: Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
    // specify type of values in <>
    public static class CommonWordsMap extends Mapper<Object, Text, Text, Text> {

        private Text filename = new Text();
        private Text word = new Text();

        protected void setup(Context context) throws IOException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String fname = split.getPath().getName();
            filename.set(fname);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("[\\s\\t\\n\\r\\f]");
            for (String s: parts) {
                if (s == "") {
                    continue;
                }
                word.set(s); // set string to text
                context.write(word, filename); // to update key, value
            }
        }
    }

    // Reducer Class takes in 4 arguments: Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
    // K, V input should be the same as K, V output of Mapper
    public static class CommonWordsReduce extends Reducer<Text, Text, IntWritable, Text> {
        private Map<String, Integer> hashmap = new HashMap<String, Integer>();
        private IntWritable freq = new IntWritable();
        private Text word = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("")) {
                return;
            }
            int file1Count = 0;
            int file2Count = 0;
            for (Text val: values) {
                String file = val.toString();
                if (file.equals("stopwords.txt")) {
                    return;
                } else if (file.equals("task1-input1.txt")) {
                    file1Count++;
                } else {
                    file2Count++;
                }
            }
            if (file1Count == 0 || file2Count == 0) {
                return;
            }
            int common = Math.min(file1Count, file2Count);
            hashmap.put(key.toString(), common);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            PriorityQueue<String> topkWords = new PriorityQueue<>(new Comparator<String>() {
                public int compare(String s1, String s2) {
                    return Integer.compare(hashmap.get(s1), hashmap.get(s2));
                }
            });

            Set<String> keys = hashmap.keySet();
            for (String key: keys) {
                topkWords.add(key);
                if (topkWords.size() > 20) {
                    topkWords.poll();
                }
            }
            // sort keys in descending order of frequency
            List<String> top20 = new ArrayList<String>(topkWords);
            Collections.sort(top20, topkWords.comparator());
            Collections.reverse(top20);

            // write top 20 to output
            while (!top20.isEmpty()) {
                String key = top20.get(0);
                top20.remove(0);
                freq.set(hashmap.get(key));
                word.set(key);
                context.write(freq, word);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        // Set Classes for MapReduce Job
        job.setJarByClass(TopkCommonWords.class); // set main class of job
        job.setMapperClass(CommonWordsMap.class); // set mapper class
        job.setReducerClass(CommonWordsReduce.class); // set reducer class

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class); // reducer key output type
        job.setOutputValueClass(Text.class); // reducer value output type

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // command line:
        // hadoop jar cm.jar TopkCommonWords input1_file input2_file stopwords_file output_file
        FileInputFormat.addInputPath(job, new Path(args[0])); // input1 file
        FileInputFormat.addInputPath(job, new Path(args[1])); // input2 file
        FileInputFormat.addInputPath(job, new Path(args[2])); // stopwords file
        FileOutputFormat.setOutputPath(job, new Path(args[3])); // output file

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 

}
