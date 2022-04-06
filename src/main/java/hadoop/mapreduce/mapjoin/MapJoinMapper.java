package hadoop.mapreduce.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> pdMap = new HashMap<>();
    private Text outK = new Text();

    //任务开始前，将pd数据缓存到pdMap
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存文件，并把文件内容封装到集合 pd.txt
        //获取缓存文件(通过缓存文件得到小表数据pd.txt)
        URI[] cacheFiles = context.getCacheFiles();//获取缓存文件地址数组
        //获取文件系统对象,并开流
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());//获取文件系统
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));//打开一个文件路径，获取文件输入流

        //从流中读取数据(通过包装流转换为reader,方便按行读取)
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        //读取数据(逐行读取，按行处理)
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {//获取一行数据!=null
            //切割
            String[] split = line.split("\t");

            //赋值
            pdMap.put(split[0], split[1]);
        }

        //关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //处理order.txt
        //读取大表数据
        String line = value.toString();
        String[] fields = line.split("\t");
        //获取pid(通过大表每行数据的pid,去pdMap里面取出pname)
        String pName = pdMap.get(fields[1]);
        //将大表每行数据的pid替换为pname
        outK.set(fields[0] + "\t" + pName + fields[2]);
        //写出
        context.write(outK, NullWritable.get());
    }

}
