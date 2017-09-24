package org.bigdata.myhadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AgePartition extends Configured implements Tool {

    /**
     * public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    public static class AgePMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapOutputKey = new Text();
        private Text mapOutputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] val = value.toString().split(",");

            mapOutputKey.set(val[2]);
            mapOutputValue.set(value);
            context.write(mapOutputKey, mapOutputValue);

        }
    }

    /**
     * public abstract class Partitioner<KEY, VALUE>
     * 自定义 shuffle阶段的 分区 partition
     */
    public static class AgePartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int num) {

            String[] val = value.toString().split(",");
            int age = Integer.parseInt(val[1]);

            if(num == 0){
                return 0;
            }
            if(age < 20){
                return 0;
            }else if(age <= 50){
                return 1 % num;
            }else{
                return 2 % num;
            }
        }
    }

    //step 2: Reduce Class

    /**
     * public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     */
    // TODO
    public static class AgePReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        private String name;
        private int age;
        private String sex;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int maxScore =0;
            for (Text t:values) {
                String[] val = t.toString().split(",");
                int score = Integer.parseInt(val[3]);
                if(score > maxScore){
                    maxScore = score;
                    name=val[0];
                    age=Integer.parseInt(val[1]);
                    sex=val[2];
                }
            }
            outputKey.set(name);
            outputValue.set(name+",age-"+age+",sex-"+sex+",score-"+maxScore);
            context.write(outputKey,outputValue);
        }
    }

    //step 3: Driver,component job
    public int run(String[] args) throws Exception {
        // step 1: get configuration
        Configuration configuration = getConf();

        //如下，可以在运行时 命令行中添加属性配置-Dxxxx
        String[] otherAgrs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherAgrs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        //本地运行时，判断输出路径是否存在，如果存在递归删除
        Path output = new Path(otherAgrs[otherAgrs.length - 1]);
        FileSystem fs = output.getFileSystem(configuration);
        fs.delete(output, true);


        //step 2: create Job
        // this.getClass().getSimpleName() 给job取个名字； 这个写法是取这个类的简单名字作为 job的名字
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());

        //step 3:run jar
        job.setJarByClass(this.getClass());

        //step 4:set job
        //input ->map->reduce->output
        // 4.1 :input
        //设置 input 路径和 分割格式
        for (int i = 0; i < otherAgrs.length - 1; ++i) {

            FileInputFormat.addInputPath(job, new Path(otherAgrs[i]));
        }

        //4.2 :map
        // TODO
        job.setMapperClass(AgePMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置自定义 partition
        job.setPartitionerClass(AgePartitioner.class);

        //4.3 :reduce
        // TODO
        job.setReducerClass(AgePReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //********************reduce 个数设置**************************************************
        //第一种：配置文件中设置 mapreduce.job.reduces
        //第二种：程序设置 默认只有一个
        job.setNumReduceTasks(3);

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

        //TODO
        int status = ToolRunner.run(configuration, new AgePartition(), args);
        System.exit(status);

    }
}
