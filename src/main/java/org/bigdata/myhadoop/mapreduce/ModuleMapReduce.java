package org.bigdata.myhadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bigdata.myhadoop.util.FileUtil;

import java.io.IOException;

/**
 *
 */
public class ModuleMapReduce extends Configured implements Tool{
    //step 1: Map Class

    /**
     * public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    // TODO
    public static class ModulMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO
        }
    }

    //step 2: Reduce Class

    /**
     * public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     */
    // TODO
    public static class ModuleReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // TODO
        }
    }

    //step 3: Driver,component job
    public int run(String[] args) throws Exception {
        // step 1: get configuration
        Configuration configuration =getConf();

        //如下，可以在运行时 命令行中添加属性配置-Dxxxx
        String[] otherAgrs=new GenericOptionsParser(configuration,args).getRemainingArgs();
        if(otherAgrs.length<2){
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        //本地运行时，判断输出路径是否存在，如果存在递归删除
        Path output=new Path(otherAgrs[otherAgrs.length-1]);
        FileSystem fs=output.getFileSystem(configuration);
        fs.delete(output,true);


        //step 2: create Job
        // this.getClass().getSimpleName() 给job取个名字； 这个写法是取这个类的简单名字作为 job的名字
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());

        //step 3:run jar
        job.setJarByClass(this.getClass());

        //step 4:set job
        //input ->map->reduce->output
        // 4.1 :input
        //设置 input 路径和 分割格式
        for (int i=0; i<otherAgrs.length-1;++i){

            FileInputFormat.addInputPath(job,new Path(otherAgrs[i]));
        }

        //4.2 :map
        // TODO
        job.setMapperClass(ModulMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //*****************************shuffle 过程设置 根据需求可选************************************************
        // 1. partitioner 设置 分区
//        job.setPartitionerClass(class);
        // 2.sort  设置排序
//        job.setSortComparatorClass(class);
        //3. map shuffle阶段的数值合并，map task的 reduce
//        job.setCombinerClass(class);
        //4.group 设置 分组
//        job.setGroupingComparatorClass(class);

        //*****************************shuffle 过程设置 根据需求可选************************************************

        //4.3 :reduce
        // TODO
        job.setReducerClass(ModuleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //********************reduce 个数设置**************************************************
        //第一种：配置文件中设置 mapreduce.job.reduces
        //第二种：程序设置 默认只有一个
//        job.setNumReduceTasks(2);

        //********************reduce 个数设置**************************************************

        //4.4: output
        FileOutputFormat.setOutputPath(job,
                new Path(otherAgrs[otherAgrs.length - 1]));

        //5: submit job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;

    }

    //step 4: run program
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        //****************************shuffle 阶段压缩设置************************************
        //第一种： 可以在mapreduce-site.xml 中配置，这就相当于给所有mapreduce任务配置了.参考官网
        //第二种：可以在程序中 设置，这样给本任务指定 需要在网上详细找下，如何设置 压缩
//        configuration.set("mapreduce.map.output.compress","true");
//        configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        //****************************shuffle 阶段压缩设置************************************

        //TODO
        int status=ToolRunner.run(configuration,new ModuleMapReduce(),args);
        System.exit(status);

    }
}
