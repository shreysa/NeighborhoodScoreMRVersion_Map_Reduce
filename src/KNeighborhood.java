/* author
Shreysa Sharma
09/24/2017
 */

package org.myorg;

import java.io.IOException;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class KNeighborhood {


    public final static IntWritable one = new IntWritable(1);
    public static final int NUM_CHARACTERS = 26;
    public static final int ASCII_START_A = 97;

    public static Logger logger;

    public static class LetterScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = value.toString()
                    .toLowerCase()
                    .replaceAll("[^a-z\\s]", "")
                    .split("\\s+");

            for (String token : tokens) {
                for (Character c : token.toCharArray()) {
                    word.set(c.toString());
                    context.write(word, one);
                }
            }
        }
    }

    public static class LetterScoreCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class LetterScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Configuration conf;
        private Map<String, Integer> letterOccurances;

        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            letterOccurances = new TreeMap<>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            letterOccurances.put(key.toString(), sum);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            long totalCharacters = 0;
            for (Integer val : letterOccurances.values()) {
                totalCharacters += val;
            }

            Integer[] letterScore = new Integer[NUM_CHARACTERS];
            for (int i = 0; i < NUM_CHARACTERS; i++) {
                Character letter = (char) (i + ASCII_START_A);
                float letterOccurance = (float) letterOccurances.get(letter.toString());
                Float percentageOccurrence = letterOccurance / (float) totalCharacters * 100.0F;

                int scoreForLetter = 0;
                if (percentageOccurrence > 10.0) {
                    scoreForLetter = 0;
                } else if (percentageOccurrence >= 8.0 && percentageOccurrence < 10.0) {
                    scoreForLetter = 1;
                } else if (percentageOccurrence >= 6.0 && percentageOccurrence < 8.0) {
                    scoreForLetter = 2;
                } else if (percentageOccurrence >= 4.0 && percentageOccurrence < 6.0) {
                    scoreForLetter = 4;
                } else if (percentageOccurrence >= 2.0 && percentageOccurrence < 4.0) {
                    scoreForLetter = 8;
                } else if (percentageOccurrence >= 1.0 && percentageOccurrence < 2.0) {
                    scoreForLetter = 16;
                } else if (percentageOccurrence < 1.0) {
                    scoreForLetter = 32;
                }

                letterScore[i] = scoreForLetter;
            }

            for (int i = 0; i < NUM_CHARACTERS; i++) {
                Character letter = (char) (i + ASCII_START_A);
                Integer val = letterScore[i];
                context.write(new Text(letter.toString()), new IntWritable(val));
            }
        }
    }



    public static void main(String [] args) throws Exception {
      logger = LoggerFactory.getLogger(KNeighborhood.class);

      String inputFilePath = args[0];
      String outputFilePath = args[1];


        logger.info("Input file: " + inputFilePath);
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "letter scores");

        job1.setJarByClass(KNeighborhood.class);

       job1.setMapperClass(LetterScoreMapper.class);
       job1.setCombinerClass(LetterScoreCombiner.class);
       job1.setReducerClass(LetterScoreReducer.class);

       job1.setOutputKeyClass(Text.class);
       job1.setOutputValueClass(IntWritable.class);

       FileInputFormat.addInputPath(job1, new Path(inputFilePath));
       FileOutputFormat.setOutputPath(job1, new Path(outputFilePath));

       System.exit(job1.waitForCompletion(true) ? 0 : 1);


    }
}
