package com.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TF_Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int words = 0;

        for (IntWritable count : counts) {
            sum += count.get();
            words++;
        }

        double tf = (double) sum / words;
        context.write(word, new DoubleWritable(tf));
    }
}
