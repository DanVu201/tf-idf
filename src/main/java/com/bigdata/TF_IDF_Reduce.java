package com.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TF_IDF_Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

    private Text word_file_key = new Text();
    private double tfidf;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double docsWithWord = 0;
        Map<String, Double> tempValues = new HashMap<>();
        for (Text value : values) {
            String[] fileCounter = value.toString().split("=");
            docsWithWord++;
            tempValues.put(fileCounter[0], Double.valueOf(fileCounter[1]));
        }

        int numOfFiles = context.getConfiguration().getInt("totalInputFiles", 0);
        double idf = Math.log10(numOfFiles / docsWithWord);
        for (String tempTfIdfFile : tempValues.keySet()) {
            this.word_file_key.set(key.toString() + "----" + tempTfIdfFile);
            this.tfidf = tempValues.get(tempTfIdfFile) * idf;
            context.write(this.word_file_key, new DoubleWritable(this.tfidf));
        }
    }
}
