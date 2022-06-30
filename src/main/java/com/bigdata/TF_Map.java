package com.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Pattern;

public class TF_Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    @Override
    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        String line = lineText.toString();
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        Text currentWord = new Text();
        for (String word : WORD_BOUNDARY.split(line)) {
            if (word.isEmpty()) {
                continue;
            }
            currentWord = new Text(word + "----" + fileName);
            context.write(currentWord, one);
        }
    }
}
