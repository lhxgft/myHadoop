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

import java.io.IOException;
import java.util.StringTokenizer;

public class MyfirstWordCount extends Configured implements Tool{


    /**
     *
     *
     * public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    public static class wordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

        private Text mapOutputKey =new Text();
        private final static IntWritable mapOutputValue=new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            //将 value 分割成 单词
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

            while (stringTokenizer.hasMoreTokens()){
                String word=stringTokenizer.nextToken();
                mapOutputKey.set(word);
                context.write(mapOutputKey,mapOutputValue);
            }
        }
    }

    /**
     *public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     */
    public static class wordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        private IntWritable reduceOutputValue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum=0;
            for (IntWritable value:values) {

                sum+=value.get();
            }
            reduceOutputValue.set(sum);
            context.write(key,reduceOutputValue);

        }
    }
    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration=getConf(); //读取配置信息

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

        //创建一个job 名字为this.getClass().getSimpleName()
        Job job =Job.getInstance(configuration , this.getClass().getSimpleName());
        //设置job jar运行时的类
        job.setJarByClass(this.getClass());
        //设置 input 路径和 分割格式
        for (int i=0; i<otherAgrs.length-1;++i){

            FileInputFormat.addInputPath(job,new Path(otherAgrs[i]));
        }

        //设置map
        job.setMapperClass(wordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        //设置reduce
        job.setReducerClass(wordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //设置output
        FileOutputFormat.setOutputPath(job,new Path(otherAgrs[otherAgrs.length-1]));

        //提交 job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        int status= ToolRunner.run(configuration,new MyfirstWordCount(),args);

        System.exit(status);

    }

}
