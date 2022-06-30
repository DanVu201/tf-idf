package com.bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import java.lang.Math;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/**
 * @author sam
 */

public class TFIDF extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TFIDF.class);

    private static final String TF_Map_Output = "TF_Map_Output";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TFIDF(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        Path InputFilepath = new Path(args[0]);
        Path OutputPath = new Path(args[1]);
        if (fs.exists(OutputPath)) {
            fs.delete(OutputPath, true);
        }
        Path TermFreqPath = new Path(TF_Map_Output);
        if (fs.exists(TermFreqPath)) {
            fs.delete(TermFreqPath, true);
        }
        FileStatus[] FilesList = fs.listStatus(InputFilepath);
        final int totalinputfiles = FilesList.length;

        Job job1 = new Job(getConf(), "TermFrequency");
        job1.setJarByClass(this.getClass());
        job1.setMapperClass(TF_Map.class);
        job1.setReducerClass(TF_Reduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, InputFilepath);
        FileOutputFormat.setOutputPath(job1, TermFreqPath);
        job1.waitForCompletion(true);

        Job job2 = new Job(getConf(), "CalculateTFIDF");
        job2.getConfiguration().setInt("totalinputfiles", totalinputfiles);
        job2.setJarByClass(this.getClass());
        job2.setMapperClass(TF_IDF_Map.class);
        job2.setReducerClass(TF_IDF_Reduce.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, TermFreqPath);
        FileOutputFormat.setOutputPath(job2, OutputPath);
        return job2.waitForCompletion(true) ? 0 : 1;
    }

}
