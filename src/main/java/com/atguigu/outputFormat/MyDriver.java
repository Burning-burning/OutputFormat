package com.atguigu.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyDriver {
    public static void main(String[] args) throws InterruptedException, ClassNotFoundException,IOException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MyDriver.class);

        job.setOutputFormatClass(MyOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("d:\\input"));
        FileOutputFormat.setOutputPath(job, new Path("d:\\output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
