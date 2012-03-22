package com.hadoop.example;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class WordIndex {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongArrayWritable> {
    private Text wordOutput = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, LongArrayWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        String word = tokenizer.nextToken();
        wordOutput.set(clean(word));
        LongArrayWritable keys = new LongArrayWritable(key);
        output.collect(wordOutput, keys);
      }
    }

  }

  public static String clean(String word) {
    // TODO tmy this is a bit ugly and probably not very performant.
    return word.replaceAll("[\"!\\.\\-\\?:,;\\(\\)]", "").replaceAll("(^')|('$)", "").toLowerCase();
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, LongArrayWritable, Text, LongArrayWritable> {
    public void reduce(Text key, Iterator<LongArrayWritable> values, OutputCollector<Text, LongArrayWritable> output, Reporter reporter) throws IOException {
      List<LongWritable> locationList = new ArrayList<LongWritable>();
      while (values.hasNext()) {
        LongArrayWritable locations = values.next();
        for (Writable location : locations.get()) {
          locationList.add((LongWritable) location);
        }
      }
      LongWritable[] locations = locationList.toArray(new LongWritable[locationList.size()]);
      output.collect(key, new LongArrayWritable(locations));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(WordIndex.class);
    conf.setJobName("wordindex");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongArrayWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileSystem dfs = FileSystem.get(conf);
    Path outputPath = new Path(args[1]);
    if (dfs.exists(outputPath)) {
      dfs.delete(outputPath, true); // true = recursively
    }
    FileOutputFormat.setOutputPath(conf, outputPath);

    JobClient.runJob(conf);
  }
}