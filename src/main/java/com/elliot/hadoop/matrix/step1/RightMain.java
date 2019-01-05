package com.elliot.hadoop.matrix.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Elliot
 * @date 2019/1/5
 */
public class RightMain {
    /**
     * 输入文件相对路径
     */
    private static String inPath = "/matrix/matrix_right.txt";

    /**
     * 输出文件相对路径
     */
    private static String outPath = "/matrix/step1_output";



    /**
     * hdfs地址
     */
    private static String hdfs = "hdfs://172.16.9.246:8020";

    public int run() {
        // 创建job配置类
        Configuration configuration = new Configuration();
        // 设置hdfs
        configuration.set("fs.defaultFS", hdfs);
        try {
            //创建一个job实例
            Job job = Job.getInstance(configuration, "matrix_step1");

            // 设置job的主类
            job.setJarByClass(RightMain.class);

            // 设置Mapper类以及输出类型
            job.setMapperClass(RightMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 设置Reducer类以及输出类型
            job.setReducerClass(RightReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // 设置输入输出路径
            FileSystem fileSystem = FileSystem.get(configuration);
            Path inputPath = new Path(inPath);
            if (fileSystem.exists(inputPath)) {
                FileInputFormat.setInputPaths(job, inputPath);
            }

            Path outputPath = new Path(outPath);
            fileSystem.delete(outputPath, true);

            FileOutputFormat.setOutputPath(job, outputPath);
            return job.waitForCompletion(true) ? 1 : -1;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        int result = new RightMain().run();
        System.out.println("step1运行" + (result == 1 ? "成功" : "失败"));
    }
}
