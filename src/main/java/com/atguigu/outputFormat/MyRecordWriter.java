package com.atguigu.outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<LongWritable, Text> {
    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    /**
     * 初始化方法
     * @param job
     */
    public void initialize(TaskAttemptContext job) throws IOException {
        String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        atguigu = fileSystem.create(new Path(outdir+"/atguigu.log"));
        other = fileSystem.create(new Path(outdir+"/other.log"));

    }

    /**
     * 将KV写出，每对KV调用一次
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String va = value.toString()+"\n";
        if (va.contains("atguigu")){
            atguigu.write(va.getBytes());

        }else{
            other.write(va.getBytes());
        }


    }

    /**
     * 关闭资源
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
