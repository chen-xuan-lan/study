package hadoop.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //1. 获取job信息
        Job job = Job.getInstance(new Configuration());
        //2. 设置加载 jar路径
        job.setJarByClass(MapJoinDriver.class);
        //3. 关联mapper
        job.setMapperClass(MapJoinMapper.class);
        //4. 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5. 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //加载缓存数据 一个文件作为缓存数据
        job.addCacheFile(new URI("file:///D:/BaiduNetdiskDownload/Hadoop3.0/资料/资料/11_input/tablecache/pd.txt"));
        //map端Join逻辑不需要reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);
        //6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\BaiduNetdiskDownload\\Hadoop3.0\\资料\\资料\\11_input\\inputtable2"));//另一个文件为输入路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\study_test\\hadoop\\mapper_join1"));
        //7. 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
