package com.haoxin.flume;

import org.apache.commons.lang.math.NumberUtils;

/**
 * @Auther Huolibin
 * @Date 2020/7/31
 * flume日志过滤工具类
 */
public class LogUtils {

    public static boolean validateEvent(String log){

        //1.切割
        String[] logContents = log.split("\\|");

        //2.j校验
        if (logContents.length !=2){
            return false;
        }

        //3.校验时间
        if (logContents[0].length() !=13 || !NumberUtils.isDigits(logContents[0])){
            return false;
        }

        //4.判断json
        if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")){
            return false;
        }

        return true;
    }

    public static boolean validateStart(String log){

        if (log == null){
            return false;
        }

        // 校验json
        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")){
            return false;
        }
        return true;
    }

}
