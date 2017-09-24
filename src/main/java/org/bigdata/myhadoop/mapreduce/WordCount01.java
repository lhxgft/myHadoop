package org.bigdata.myhadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.myhadoop.util.FileUtil;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 */
public class WordCount01 {
    //step 1: Map Class

    /**
     * public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text mapOutputKey = new Text();
        private final static IntWritable mapOutputValue = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //1:split
            //标准包中 分割 字符串
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

            //2:iterator
            while (stringTokenizer.hasMoreTokens()) {
                //get word value
                String wordValu = stringTokenizer.nextToken();
                //set value
                mapOutputKey.set(wordValu);
                //output
                context.write(mapOutputKey, mapOutputValue);
            }
        }
    }

    //step 2: Reduce Class

    /**
     * public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outputvalue = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //sum tmp
            int sum = 0;
            //iterator
            for (IntWritable value : values) {

                sum+=value.get(); //转换成 int类型
            }
            //set value
            outputvalue.set(sum);
            //output
            context.write(key,outputvalue);


        }
    }

    //step 3: Driver,component job
    public int run(String[] args) throws Exception {
        //判断路径是否存在，如果存在就直接删除。 这个方法只用在本地调试的时候。
        FileUtil.deleteDir(args[1]);

        // step 1: get configuration
        Configuration configuration = new Configuration();

        //step 2: create Job
        /**
         * this.getClass().getSimpleName() 给job取个名字； 这个写法是取这个类的简单名字作为 job的名字
         */
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());

        //step 3:run jar
        job.setJarByClass(this.getClass());

        //step 4:set job
        //input ->map->reduce->output
        // 4.1 :input
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);

        //4.2 :map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //4.3 :reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //4.4: output
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        //5: submit job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;

    }

    //step 4: run program
    public static void main(String[] args) throws Exception {

        int status = new WordCount01().run(args);
        System.exit(status);

    }
}
