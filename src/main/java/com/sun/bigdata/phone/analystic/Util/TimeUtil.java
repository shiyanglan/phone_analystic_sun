/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TimeUtil
 * Author:   Chenfg
 * Date:     2020/4/16 0016 15:01
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.sun.bigdata.phone.analystic.Util;

import org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Chenfg
 * @create 2020/4/16 0016
 * @since 1.0.0
 *
 * 时间工具类
 */
public class TimeUtil {
    private static final String DEFAULT_FORMAT="yyyy-MM-dd";

    /**
     * 获取默认的日期昨天
     * --格式 yyyy-MM-dd
     * @return
     */
    public static String getYesterday() {
        return getYesterday(DEFAULT_FORMAT);
    }

    /**
     * 获取指定格式的日期
     * @param pattern
     * @return
     */
    public static String getYesterday(String pattern) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);

        Calendar calendar = Calendar.getInstance();

        calendar.add(Calendar.DAY_OF_YEAR,-1);

        return dateFormat.format(calendar.getTime());
    }

    /**
     * 通过正则表示是来判断日期格式是否正确
     * @param date
     * @return
     */
    public static boolean isValidDate(String date) {
        String regexp = "^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}";
        Matcher matcher= null;
        Boolean res = false;
        if(StringUtils.isNotEmpty(date)){
            Pattern pattern = Pattern.compile(regexp);
            matcher = pattern.matcher(date);
        }

        if(matcher!=null){
            res = matcher.matches();
        }

        return res;
    }
}
