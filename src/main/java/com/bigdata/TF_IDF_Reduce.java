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

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double docswithword = 0;
        Map<String, Double> tempvalues = new HashMap<>();
        for (Text v : values) {
            String[] filecounter = v.toString().split("=");
            docswithword++;
            tempvalues.put(filecounter[0], Double.valueOf(filecounter[1]));
        }

        int numoffiles = context.getConfiguration().getInt("totalinputfiles", 0);
        double idf = Math.log10(numoffiles / docswithword);
        for (String temp_tfidf_file : tempvalues.keySet()) {
            this.word_file_key.set(key.toString() + "----" + temp_tfidf_file);
            this.tfidf = tempvalues.get(temp_tfidf_file) * idf;
            context.write(this.word_file_key, new DoubleWritable(this.tfidf));
        }
    }
}
