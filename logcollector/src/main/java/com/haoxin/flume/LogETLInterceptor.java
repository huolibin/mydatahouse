package com.haoxin.flume;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @Auther Huolibin
 * @Date 2020/7/31
 * etl拦截器
 * 来简单的清洗数据
 */
public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1.获取数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        //2.校验 启动日志   事件日志
        if (log.contains("start")){
            //校验启动日志
            if (LogUtils.validateStart(log)){
                return event;
            }

        }else {
            //校验 事件日志
            if (LogUtils.validateEvent(log)){
                return event;
            }
        }
        return null;
    }

    /**
     *处理多个event，判读不为空的event 加入list中
     */
    @Override
    public List<Event> intercept(List<Event> list) {

        ArrayList<Event> events = new ArrayList<>();
        for (Event e:list
             ) {
            Event intercept1 = intercept(e);
            if (intercept1 != null){
                events.add(intercept1);
            }

        }
        return events;
    }

    @Override
    public void close() {

    }

    /**
     * 建静态内部类builder
     */
    public static class Builder implements Interceptor.Builder{
        @Override
        public void configure(Context context) {

        }

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }
    }
}
