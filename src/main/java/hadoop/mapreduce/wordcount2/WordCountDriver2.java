package hadoop.mapreduce.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver2 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2.设置jar包路径
        job.setJarByClass(WordCountDriver2.class);

        // 3.关联mapper和reducer
        job.setMapperClass(WordCountMapper2.class);
        job.setReducerClass(WordCountReducer2.class);

        // 4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果不设置InputFormat,它默认使用TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);

        //虚拟存储切片最大值设置4m
//        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

        //虚拟存储切片最大值设置20m
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        // 6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(""));
        FileOutputFormat.setOutputPath(job, new Path(""));

        // 7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
