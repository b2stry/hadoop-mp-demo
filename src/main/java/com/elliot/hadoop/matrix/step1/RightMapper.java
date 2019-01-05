package com.elliot.hadoop.matrix.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Elliot
 * @date 2019/1/5
 */
public class RightMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key : 1
     * value : 1	1_0,2_3,4_-1,4_2,5_-3
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rowAndLine = value.toString().split("\t");

        // 矩阵的行号
        String row = rowAndLine[0];
        String[] lines = rowAndLine[1].split(",");

        //["1_0", "2_3", "3_-1", "4_2", "5_-3"]
        for (int i = 0; i < lines.length; i++) {
            String[] number = lines[i].split("_");
            String column = number[0];
            String valueStr = number[1];

            //key: 列号    value:行号_值
            outKey.set(column);
            outValue.set(row + "_" + valueStr);
            context.write(outKey, outValue);
        }
    }
}
