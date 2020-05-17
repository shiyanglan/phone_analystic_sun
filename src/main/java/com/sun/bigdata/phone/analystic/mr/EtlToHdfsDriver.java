/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: EtlToHdfsDriver
 * Author:   Chenfg
 * Date:     2020/4/16 0016 11:41
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.sun.bigdata.phone.analystic.mr;

import com.sun.bigdata.phone.analystic.Util.TimeUtil;
import com.sun.bigdata.phone.analystic.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 *
 * @author Chenfg
 * @create 2020/4/16 0016
 * @since 1.0.0
 * 日期需要由外部传入
 * 由数据在hdfs的目录结构决定
 * 清洗之前的数据
 *  /logs/04/16
 *  /logs/04/17
 *  .....
 *  清洗后的数据
 *  /ods/04/16
 *  /ods/04/17
 *
 *  执行清洗流程需要按天进行，参数
 *  yarn jar .jar package.classname -d 2020-04-16
 */
public class EtlToHdfsDriver implements Tool {
    private static final Logger logger =Logger.getLogger(EtlToHdfsDriver.class);
    private Configuration conf = new Configuration();

    //主函数
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new EtlToHdfsDriver(), args);
        } catch (Exception e) {
            logger.error("执行任务失败",e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
//        Configuration conf1 = getConf();

        //1、获取-d之后的日期把那个存储到conf中，如果没有-d或者日期格式不对则使用默认时间--昨天
        handleArgs(conf,args);

        //设置连接参数
        conf.set(GlobalConstants.CONNECT_KEY,GlobalConstants.CONNECT_VALUE);
        conf.set(GlobalConstants.FRAMEWORK_NAME,GlobalConstants.FRAMEWORK_VALUE);

        //获取Job对象
        Job job = Job.getInstance(conf, "etl");

        //设置执行路径
        job.setJarByClass(EtlToHdfsDriver.class);

        //设置map相关的参数
        job.setMapperClass(EtlTohdfsMapper.class);
        job.setOutputKeyClass(LogWritable.class);
        job.setOutputValueClass(NullWritable.class);

        //设置reduce的数量为0
        job.setNumReduceTasks(0);

        //设置输入输出路径
        handleInputOutput(job);

        return job.waitForCompletion(true)?0:1;
    }

    /**
     * 处理输入输出路径
     * @param job
     */
    private void handleInputOutput(Job job) {
        String[] fields = job.getConfiguration().get(GlobalConstants.RUNNING_DATE).split("-");

        String month = fields[1];
        String day = fields[2];

        String input = "/logs/"+month+"/"+day;
        String output = "/ods/"+month+"/"+day;

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());

            Path inPath = new Path(input);
            Path outPath = new Path(output);

            //判断输入路径是否存在
            if(fs.exists(inPath)){
                FileInputFormat.setInputPaths(job,inPath);
            }else{
                throw new RuntimeException("输入路径不存在，请检查"+inPath.toString());
            }

            //判断输出路径是否存在，如果存在，则先删除
            if(fs.exists(outPath)){
                fs.delete(outPath,true);
            }

            //设置输出路径
            FileOutputFormat.setOutputPath(job,outPath);
        } catch (IOException e) {
            logger.error("设置输入输出路径失败",e);
        }
    }

    private void handleArgs(Configuration conf, String[] args) {
        String date = null;

        //判断参数列表
        if(args.length > 0){
            //循环
            for (int i = 0; i < args.length; i++) {
                //判断参数中是否包含-d
                if(args[i].equals("-d")){
                    if (i+1 < args.length){
                        date = args[i+1];
                        break;
                    }
                }
            }
        }

        //判断date是否为空 是否合法
        if(StringUtils.isEmpty(date) || TimeUtil.isValidDate(date)){
            date = TimeUtil.getYesterday();
        }

        //将date存储到conf中
        conf.set(GlobalConstants.RUNNING_DATE,date);
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
