/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: userAgentUtil
 * Author:   Chenfg
 * Date:     2020/4/15 0015 16:07
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.sun.bigdata.phone.analystic.etl;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 *
 * @author Chenfg
 * @create 2020/4/15 0015
 * @since 1.0.0
 */
public class userAgentUtil {
    private static final Logger logger = Logger.getLogger(userAgentUtil.class);

    private static UASparser uaSparser = null;
    static{
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.error("获取userAgent解析对象失败",e);
        }
    }

    public static UserAgentInfo parserUserAgent(String userAgent){
        UserAgentInfo ua = null;
        if(StringUtils.isNotEmpty(userAgent)){
            try {
                cz.mallat.uasparser.UserAgentInfo info = uaSparser.parse(userAgent);
                if(info!=null){
                    ua = new UserAgentInfo();
                    //将info中解析出来的信息取出来赋值给ua
                    ua.setBrowserName(info.getUaFamily());
                    ua.setBrowserVersion(info.getBrowserVersionInfo());
                    ua.setOsName(info.getOsFamily());
                    ua.setOsVersion(info.getOsCompany());

                    System.out.println(info.toString());
                }
            } catch (IOException e) {
                logger.error("解析userAgent失败",e);
            }
        }

        return ua;
    }

    //封装userAgent解析之后的信息
    public static class UserAgentInfo{
        private String browserName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        public UserAgentInfo() {
        }

        public UserAgentInfo(String browserName, String browserVersion, String osName, String osVersion) {
            this.browserName = browserName;
            this.browserVersion = browserVersion;
            this.osName = osName;
            this.osVersion = osVersion;
        }

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}
