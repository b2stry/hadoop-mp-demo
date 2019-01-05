package com.elliot.hadoop.matrix.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Elliot
 * @date 2019/1/5
 */
public class LeftMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    private List<String> cacheList = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //通过输入流将全局缓存中的右侧矩阵读入List<String>中
        FileReader fr = new FileReader("matrix2/part-r-00000");
        BufferedReader br = new BufferedReader(fr);

        // 每一行格式：行 tab 列_值,列_值,列_值,列_值
        String line;
        while ((line = br.readLine()) != null) {
            cacheList.add(line);
        }

        fr.close();
        br.close();
    }

    /**
     * key : 1
     * value : 1	1_1,2_2,3_-2,4_0
     *
     * @param key：行号
     * @param value：  行 tab 列_值,列_值,列_值,列_值
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 行
        String rowMatrix1 = value.toString().split("\t")[0];
        //列_值
        String[] columnValueArrayMatrix1 = value.toString().split("\t")[1].split(",");

        for (String line : cacheList) {
            // 右侧矩阵 行
            String rowMatrix2 = line.split("\t")[0];
            //右侧矩阵  行 tab 列_值,列_值,列_值,列_值
            String[] columnValueArrayMatrix2 = line.split("\t")[1].split(",");
            // 矩阵两行相乘得到的结果
            int result = 0;
            // 遍历做矩阵的第一行的每一列
            for (String columnValue : columnValueArrayMatrix1) {
                String columnMatrix1 = columnValue.split("_")[0];
                String valueMatrix1 = columnValue.split("_")[1];

                // 遍历右矩阵每一行的每一列
                for (String columnValue2 : columnValueArrayMatrix2) {
                    if (columnValue2.startsWith(columnMatrix1 + "_")) {
                        String valueMatrix2 = columnValue2.split("_")[1];
                        // 将两列的值相乘 并累加
                        result += Integer.valueOf(valueMatrix1) * Integer.valueOf(valueMatrix2);
                    }
                }
            }

            //result 是结果矩阵中的某元素，坐标是   行 rowMatrix1, 列  rowMatrix2（因为右矩阵已经转置）
            outKey.set(rowMatrix1);
            outValue.set(rowMatrix2 + "_" + result);
            context.write(outKey, outValue);
        }
    }
}
