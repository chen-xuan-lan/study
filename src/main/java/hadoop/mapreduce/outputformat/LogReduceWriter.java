package hadoop.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogReduceWriter extends RecordWriter<Text, NullWritable> {


    private FSDataOutputStream otherOut;
    private FSDataOutputStream atguiguOut;

    public LogReduceWriter(TaskAttemptContext job) {
        //创建两条流
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());

            atguiguOut = fileSystem.create(new Path("D:\\study_test\\hadoop\\outputformat\\atguigu.log"));

            otherOut = fileSystem.create(new Path("D:\\study_test\\hadoop\\outputformat\\other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件内容写出
     *
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        //具体写
        if (log.contains("atguigu")) {
            atguiguOut.writeBytes(log + "\n");
        } else {
            otherOut.writeBytes(log + "\n");
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关流
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
