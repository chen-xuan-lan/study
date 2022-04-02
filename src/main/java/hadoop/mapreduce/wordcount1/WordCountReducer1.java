package hadoop.mapreduce.wordcount1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN: reduce阶段输入的key类型(即输入数据的偏移量0,1,2...): Text
 * VALUEIN: reduce阶段输入的value类型: IntWritable
 * KEYOUT: reduce阶段输出的key类型: Text
 * VALUEOUT: reduce阶段输出的value类型(统计次数): IntWritable
 */
public class WordCountReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //累加
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);

        //写出
        context.write(key, outV);
    }
}
