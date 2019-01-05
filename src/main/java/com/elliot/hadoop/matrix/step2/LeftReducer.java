package com.elliot.hadoop.matrix.step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author Elliot
 * @date 2019/1/5
 */
public class LeftReducer extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key: 行号   value: [行号_值, 行号_值, 行号_值...]
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();

        for (Text value : values) {
            // value: 行号_值
            sb.append(value + ",");
        }

        String line = sb.toString();
        if (line.endsWith(",")) {
            line = line.substring(0, line.length() - 1);
        }

        outKey.set(key);
        outValue.set(line);
        context.write(outKey, outValue);
    }
}
