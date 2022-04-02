package hadoop.mapreduce.wordcount1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: map阶段输入的key类型(即输入数据的偏移量0,1,2...): LongWritable
 * VALUEIN: map阶段输入的value类型: Text
 * KEYOUT: map阶段输出的key类型: Text
 * VALUEOUT: map阶段输出的value类型(统计次数): IntWritable
 */
public class WordCountMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();

        //2.切割
        String[] words = line.split(" ");

        //3.循环写出
        for (String word : words) {

            outK.set(word);

            context.write(outK, outV);
        }
    }
}
