package com.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TF_Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count : counts) {
            sum += count.get();
        }
        double tf = Math.log10(10) + Math.log10(sum);
        context.write(word, new DoubleWritable(tf));
    }
}
