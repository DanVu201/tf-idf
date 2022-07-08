package com.bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class TFIDF extends Configured implements Tool {

    private static final String TF_Map_Output = "TF_Map_Output";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TFIDF(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        FileSystem fs = FileSystem.get(getConf());

        Path inputFilePath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Path termFreqPath = new Path(TF_Map_Output);

        if (fs.exists(termFreqPath)) {
            fs.delete(termFreqPath, true);
        }

        FileStatus[] filesList = fs.listStatus(inputFilePath);
        final int totalInputFiles = filesList.length;

        Job job1 = new Job(getConf(), "TermFrequency");
        job1.setJarByClass(this.getClass());
        job1.setMapperClass(TF_Map.class);
        job1.setReducerClass(TF_Reduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, inputFilePath);
        FileOutputFormat.setOutputPath(job1, termFreqPath);
        job1.waitForCompletion(true);

        Job job2 = new Job(getConf(), "CalculateTFIDF");
        job2.getConfiguration().setInt("totalInputFiles", totalInputFiles);
        job2.setJarByClass(this.getClass());
        job2.setMapperClass(TF_IDF_Map.class);
        job2.setReducerClass(TF_IDF_Reduce.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, termFreqPath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

}
