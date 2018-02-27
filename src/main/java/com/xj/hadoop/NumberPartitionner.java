package com.xj.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;

/**
 * 按照数量分区
 *
 * @author xiangjing
 * @date 2018/2/27
 * @company 天极云智
 */
public class NumberPartitionner extends Partitioner<Text,IntWritable> {

    private static final Logger log= LoggerFactory.getLogger(WordCountJob2.class);

    private static Map<String,Integer> map = new HashMap<>();

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        log.info(text+":"+intWritable);

        if(intWritable.get()>1){
            return  00001;
        }else{
            return  00000;
        }
    }
}
