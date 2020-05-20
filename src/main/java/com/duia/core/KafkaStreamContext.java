package com.duia.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName KafkaStreamContext
 * @Author xiaoyu
 * @Date 2020/5/18 9:53
 * @Description TODO
 **/
@Component
public class KafkaStreamContext implements ApplicationContextAware {
    private static final Logger LOGGER =  LoggerFactory.getLogger(KafkaStreamContext.class);
    private static KafkaStreamManager manager;
    private static ApplicationContext context;
    private static final Map<String,KafkaStreamApplication> apps = new HashMap<>();
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (context==null){
            context = applicationContext;
        }
        //创建manager
        if (manager == null){
            manager = new KafkaStreamManagerImpl();
        }
        Map<String, Object> beans = context.getBeansWithAnnotation(KafkaStream.class);
        for (Object o:beans.values()){
            if (o instanceof KafkaStreamApplication){
                KafkaStreamApplication kafkaStreamApp = (KafkaStreamApplication)o;
                KafkaStream annotation = kafkaStreamApp.getClass().getAnnotation(KafkaStream.class);
                if (annotation!=null){
                    kafkaStreamApp.setStat(annotation.stat());
                }
                apps.put(kafkaStreamApp.getApplicationName(),kafkaStreamApp);
            }else{
                LOGGER.warn("{} 需要实现KafkaStreamApplication接口",o.getClass().getName());
            }
        }
        //将kafkaStreamApp注册到zookeeper
        for (KafkaStreamApplication app:apps.values()){
            manager.registerKafkaStreamApplication(app);
            manager.watchKafkaStreamApplication(app);
        }
    }

    /**获取spring容器applicationContext
     * @return
     */
    public ApplicationContext getSpringApplicationContext(){
        return context;
    }
    /**
     * 获取一个KafkaStreamManager
     * @return
     */
    public KafkaStreamManager getKafkaStreamManager(){
        return manager;
    }

    /**
     * @return 获取所有的kafka stream application
     */
    public Map<String,KafkaStreamApplication> getAllKafkaStreamApp() {
        return apps;
    }

    /**
     * 根据applicationId 获取对应的application
     *
     * @param appName
     * @return
     */
    public KafkaStreamApplication getKafkaStreamApp(String appName) {
        return apps.get(appName);
    }
}
