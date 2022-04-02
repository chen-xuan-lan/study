package hadoop.hdfs.utils.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 客户端代码常用思路
 * 1、获取一个客户端
 * 2、执行相关操作命令
 * 3、关闭资源
 * 典型技术：HDFS、Zookeeper
 */
public class HdfsClient {
    private FileSystem fileSystem;

    @Before
    public void initClient() throws URISyntaxException, IOException, InterruptedException {
        //windows 环境远程访问集群 要获取到集群对应的NameNode的主机地址和端口号
        //hdfs://hadoop102:8020  hdfs:hdfs协议 hadoop102:8020  NameNode节点所在主机每部通信端口号8020
        //链接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        //设置用户
        String user = "test_user";
        //1.获取客户端对象
        fileSystem = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        //3.关闭资源
        fileSystem.close();
    }

    /**
     * 创建文件夹
     */
    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {
        //2.创建一个文件夹
        fileSystem.mkdirs(new Path("/study/hdfstest"));
    }

    /**
     * 文件上传
     * 配置参数优先级
     * hdfs.default.xml => hdfs-site.xml => 在项目资源目录下的hdfs-site.xml配置文件 => 代码中的配置
     * 注：代码中的配置指：创建客户端时，创建配置文件  Configuration configuration = new Configuration();  configuration.set("dfs.replication","2");
     */
    @Test
    public void testFileUpload() throws IOException {
        //参数解读：
        // 参数一delSrc:是否删除本地原数据
        // 参数二overwrite:目标路径上的文件是否允许覆盖  注：如果目标路径已存在上传的文件，且允许覆盖设置为false，则会报错：此文件已存在
        // 参数三src:原数据路径
        // 参数四dst:目的路径  目的路径也可写为全路径 hdfs://hadoop102/study/hdfstest
        fileSystem.copyFromLocalFile(false, true, new Path("E:\\自动化测试结果.xls"), new Path("/study/hdfstest"));
    }

    /**
     * 文件下载
     * boolean delSrc:是否删除源文件
     * Path src:源文件路径 也可写为全路径 hdfs://hadoop102/study/hdfstest
     * Path dst:目标地址路径
     * boolean useRawLocalFileSystem:是否开启本地文件校验
     * 文件下载到本地后，有两个文件
     * 1.crc文件 校验数据的方式
     */
    @Test
    public void testFileDownload() throws IOException {
        fileSystem.copyToLocalFile(false, new Path("/study/hdfstest"), new Path("E:\\错误日志"), true);
    }

    /**
     * 文件删除
     * Path var1:删除路径
     * boolean var2:是否递归删除
     */
    @Test
    public void testFileRemove() throws IOException {
        //删除文件
        fileSystem.delete(new Path(""), false);

        //删除空目录
        fileSystem.delete(new Path(""), false);

        //删除非空目录 注：非空目录不递归删除 false 会报错 需要改成递归删除 true
        fileSystem.delete(new Path(""), false);
    }

    /**
     * 文件更名和移动
     * Path var1:原文件路径
     * Path var2：目标文件路径
     */
    @Test
    public void testRenameAndMove() throws IOException {
        //文件更名
        fileSystem.rename(new Path(""), new Path(""));

        //文件更名并移动
        fileSystem.rename(new Path(""), new Path(""));

        //目录更名
        fileSystem.rename(new Path(""), new Path(""));
    }

    /**
     * 获取文件详细信息
     * Path f:要查看的文件路径
     * boolean recursive:是否递归
     */
    @Test
    public void testGetFileDetail() throws IOException {
        //获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        //遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("======" + fileStatus.getPath() + "======");
            //权限
            System.out.println(fileStatus.getPermission());
            //文件所有者
            System.out.println(fileStatus.getOwner());
            //文件所属组
            System.out.println(fileStatus.getGroup());
            //文件大小
            System.out.println(fileStatus.getLen());
            //上次修改时间
            System.out.println(fileStatus.getModificationTime());
            //文件副本
            System.out.println(fileStatus.getReplication());
            //文件块大小
            System.out.println(fileStatus.getBlockSize());
            //文件名称
            System.out.println(fileStatus.getPath().getName());

            //获取块信息（获取块的存储位置）
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    /**
     * 判断是文件夹还是文件
     * Path var1:要判断的路径
     */
    @Test
    public void testIsFolderOrFile() throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            //判断是否为文件
            if (status.isFile()) {
                System.out.println("文件名===" + status.getPath().getName());
            } else {
                System.out.println("目录名===" + status.getPath().getName());
            }
        }
    }

}
