package com.bigdata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TF_IDF_Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text word_key = new Text();
    private Text filename_tf = new Text();

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] word_file_tf = value.toString().split("\t");
        String[] word_file = word_file_tf[0].split("----");
        this.word_key.set(word_file[0]);
        this.filename_tf.set(word_file[1] + " = " + word_file_tf[1]);
        context.write(word_key, filename_tf);
    }
}