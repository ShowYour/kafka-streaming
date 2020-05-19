package com.duia.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.List;
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
        //创建manager
        if (context==null){
            context = applicationContext;
        }
        Map<String, Object> beans = context.getBeansWithAnnotation(KafkaStream.class);
        for (Object o:beans.values()){
            if (o instanceof KafkaStreamApplication){
                KafkaStreamApplication kafkaStreamApp = (KafkaStreamApplication)o;
                apps.put(kafkaStreamApp.getApplicationName(),kafkaStreamApp);
            }else{
                LOGGER.warn("{} 需要实现KafkaStreamApplication接口",o.getClass().getName());
            }
        }
        if (manager == null){
            manager = new KafkaStreamManagerImpl();
        }
        //将kafkaStreamApp注册到zookeeper
        for (KafkaStreamApplication app:apps.values()){
            manager.registerKafkaStreamApplication(app);
            manager.watchKafkaStreamApplication(app);
        }
    }

    /**
     * 获取一个KafkaStreamManager
     * @return
     */
    public static KafkaStreamManager getKafkaStreamManager(){
        return manager;
    }

    /**
     * @return 获取所有的kafka stream application
     */
    static List<KafkaStreamApplication> getAllKafkaStreamApp() {
        return null;
    }

    /**
     * 根据applicationId 获取对应的application
     *
     * @param appName
     * @return
     */
    static KafkaStreamApplication getKafkaStreamApp(String appName) {
        return null;
    }
}
