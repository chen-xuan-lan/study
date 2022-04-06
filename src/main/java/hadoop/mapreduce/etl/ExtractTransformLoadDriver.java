package hadoop.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ExtractTransformLoadDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args= new String[]{"D:/BaiduNetdiskDownload/Hadoop3.0/资料/资料/11_input/inputlog","D:/study_test/hadoop/etl"};
        //1. 获取job信息
        Job job = Job.getInstance(new Configuration());
        //2. 加载jar包
        job.setJarByClass(ExtractTransformLoadDriver.class);
        //3. 关联map
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置reducetask个数为0
        job.setNumReduceTasks(0);
        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
