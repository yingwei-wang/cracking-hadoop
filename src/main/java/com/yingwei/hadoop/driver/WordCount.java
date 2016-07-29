package com.yingwei.hadoop.driver;

import com.yingwei.hadoop.mrunit.WordCountMapper;
import com.yingwei.hadoop.mrunit.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool {


    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount.class);

        //setup input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        //mapper
        job.setMapperClass(WordCountMapper.class);
        //reducer
        job.setReducerClass(WordCountReducer.class);
        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        //execute
        boolean res = job.waitForCompletion(true);

        if (res) {
            return 0;
        } else {
            return 1;
        }
    }


    public static void main(String... args) throws Exception {
        int res = ToolRunner.run(new WordCount(), args);
        System.exit(res);
    }


}
