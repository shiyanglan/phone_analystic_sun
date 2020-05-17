/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: EtlTohdfsMapper
 * Author:   Chenfg
 * Date:     2020/4/15 0015 16:38
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.sun.bigdata.phone.analystic.mr;

import com.sun.bigdata.phone.analystic.common.Constants;
import com.sun.bigdata.phone.analystic.etl.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 *
 * @author Chenfg
 * @create 2020/4/15 0015
 * @since 1.0.0
 */
public class EtlTohdfsMapper extends Mapper<LongWritable, Text,LogWritable, NullWritable> {
    private static final Logger logger =Logger.getLogger(EtlTohdfsMapper.class);

    private LogWritable k = new LogWritable();
//    private int inputrecords,filterrecords,outputrecords = 0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();

            //统计总的输入行数
            //通过动态设置自定义计数器
            context.getCounter("customerCount", "inputRecords").increment(1);

            //空行处理
            if (StringUtils.isEmpty(line)) {
                context.getCounter("customerCount", "filterRecords").increment(1);
                return;
            }

            //调用LogUtil中的 parserLog方法，返回map，然后循环map输出
            Map<String, String> map = LogUtil.parserLog(line);

            if(map.size() == 0){
                context.getCounter("customerCount", "filterRecords").increment(1);
                return;
            }

            //数据按照事件来进行区分
            //根据数据量、业务需求（大量按照事件分别统计的指标）
            //获取事件名
            String eventName = map.get(Constants.LOG_EVENT_NAME);
            Constants.EventEnum event = Constants.EventEnum.valueOfAlias(eventName);

            switch(event){
                case LANUCH:
                case EVENT:
                case PAGEVIEW:
                case CHARGEREQUEST:
                case CHARGESUCCESS:
                case CHARGEREFUND:
                    //处理数据输出
                    handleLog(map,context);
                default:
            }
        }catch(Exception e){
            context.getCounter("customerCount", "wrongRecords").increment(1);
        }
    }

    /**
     * 处理真正的将map中的所有kv数据输出
     * @param map
     * @param context
     */
    private void handleLog(Map<String, String> map, Context context) {
        try {
            //循环迭代map
            for (Map.Entry<String,String> en:map.entrySet()) {
                switch (en.getKey()){
                    case "ver": k.setVer(en.getValue()); break;
                    case "s_time": k.setS_time(en.getValue()); break;
                    case "en": k.setEn(en.getValue()); break;
                    case "u_ud": k.setU_ud(en.getValue()); break;
                    case "u_mid": k.setU_mid(en.getValue()); break;
                    case "u_sd": k.setU_sd(en.getValue()); break;
                    case "c_time": k.setC_time(en.getValue()); break;
                    case "l": k.setL(en.getValue()); break;
                    case "b_iev": k.setB_iev(en.getValue()); break;
                    case "b_rst": k.setB_rst(en.getValue()); break;
                    case "p_url": k.setP_url(en.getValue()); break;
                    case "p_ref": k.setP_ref(en.getValue()); break;
                    case "tt": k.setTt(en.getValue()); break;
                    case "pl": k.setPl(en.getValue()); break;
                    case "ip": k.setIp(en.getValue()); break;
                    case "oid": k.setOid(en.getValue()); break;
                    case "on": k.setOn(en.getValue()); break;
                    case "cua": k.setCua(en.getValue()); break;
                    case "cut": k.setCut(en.getValue()); break;
                    case "pt": k.setPt(en.getValue()); break;
                    case "ca": k.setCa(en.getValue()); break;
                    case "ac": k.setAc(en.getValue()); break;
                    case "kv_": k.setKv_(en.getValue()); break;
                    case "du": k.setDu(en.getValue()); break;
                    case "browserName": k.setBrowserName(en.getValue()); break;
                    case "browserVersion": k.setBrowserVersion(en.getValue()); break;
                    case "osName": k.setOsName(en.getValue()); break;
                    case "osVersion": k.setOsVersion(en.getValue()); break;
                    case "country": k.setCountry(en.getValue()); break;
                    case "province": k.setProvince(en.getValue()); break;
                    case "city": k.setCity(en.getValue()); break;
                }
            }

            context.write(k,NullWritable.get());
            context.getCounter("customerCount", "outputRecords").increment(1);
        } catch (IOException e) {
            context.getCounter("customerCount", "wrongRecords").increment(1);
        } catch (InterruptedException e) {
            context.getCounter("customerCount", "wrongRecords").increment(1);
        }
    }
}
