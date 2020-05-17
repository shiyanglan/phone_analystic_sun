/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: LogUtil
 * Author:   Chenfg
 * Date:     2020/4/16 0016 9:49
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.sun.bigdata.phone.analystic.etl;

import com.sun.bigdata.phone.analystic.common.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Chenfg
 * @create 2020/4/16 0016
 * @since 1.0.0
 * 统一抽取工具类，对需要清洗的日志数据进行解析
 */
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);

    /**
     * 58.59.132.227^A1535530816.321^A221.13.21.192^A/shopping.jsp?c_time=1535530816321&oid=123458&u_mid=ec26f9ea-d80e-4ed4-a845-47b0aa7b3a3f&pl=java_server&en=e_cs&sdk=jdk&ver=1
     * @param log
     */
    public static Map<String,String> parserLog(String log) throws IOException {
        Map<String,String> map = new ConcurrentHashMap<String,String>();

        //判断传入参数
        if(StringUtils.isNotEmpty(log)){
            String[] fields = log.split("\\^A");

            //判断字段数是否满足4个
            if(fields.length == 4){
                //将数据存入map
                map.put(Constants.LOG_IP,fields[0]);
                map.put(Constants.LOG_SERVER_TIME,fields[1].replaceAll("\\.",""));

                //参数列表
                String params = fields[3];

                //处理params
                handlerParams(params,map);

                //处理ip解析
                handlerIP(map);

                //处理userAgent
                handlerUserAgent(map);
            }
        }

        return map;
    }

    /**
     * 将map中的b_iev取出来，解析成userAgent信息，存储到map中
     * @param map
     */
    private static void handlerUserAgent(Map<String, String> map) throws IOException {
        if(map.containsKey(Constants.LOG_USERAGENT)){
            userAgentUtil.UserAgentInfo info = userAgentUtil.parserUserAgent(map.get(Constants.LOG_USERAGENT));

            //将解析的结果存储到map中
            map.put(Constants.LOG_BROWSER_NAME,info.getBrowserName());
            map.put(Constants.LOG_BROWSER_VERSION,info.getBrowserVersion());
            map.put(Constants.LOG_OS_NAME,info.getOsName());
            map.put(Constants.LOG_OS_VERSION,info.getOsVersion());
        }
    }

    /**
     * 将map中的ip取出来，然后解析成国家省市信息，然后存储到map中
     * @param map
     */
    private static void handlerIP(Map<String, String> map) {
        if(map.containsKey(Constants.LOG_IP)){
            IPUtil.RegionInfo info = IPUtil.getRegionInfoByIP(map.get(Constants.LOG_IP));

            //将解析后的地区信息存储到map中
            map.put(Constants.LOG_COUNTRY,info.getCountry());
            map.put(Constants.LOG_PROVINCE,info.getProvince());
            map.put(Constants.LOG_CITY,info.getCity());
        }
    }

    /**
     * 解析url传入的参数
     * /shopping.jsp?c_time=1535530816321&oid=123458&u_mid=ec26f9ea-d80e-4ed4-a845-47b0aa7b3a3f&pl=java_server&en=e_cs&sdk=jdk&ver=1
     * @param params
     * @param map
     */
    private static void handlerParams(String params, Map<String, String> map) throws UnsupportedEncodingException {
            if(StringUtils.isNotEmpty(params)){
                int index = params.indexOf("?");

                if(index > 0){
                    //以&分割参数
                    String fields[] = params.substring(index+1).split("&");

                    //循环遍历
                    for (String field : fields) {
                        String kvs[] = field.split("=");
                        String k = kvs[0];
                        String v = URLDecoder.decode(kvs[1],"utf-8");

                        //判断key是否为空
                        if(StringUtils.isNotEmpty(k)){
                            //存储到map中
                            map.put(k,v);
                        }
                    }
                }
            }
    }
}
