package com.duia.core;

import com.duia.util.ZkUtils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import com.duia.core.KafkaStreamApplication.KafkaStreamApplicationStat;

/**
 * @ClassName KafkaStreamManagerImpl
 * @Author xiaoyu
 * @Date 2020/5/15 18:37
 * @Description TODO
 **/
public class KafkaStreamManagerImpl implements KafkaStreamManager {
    private static final Logger LOGGER =  LoggerFactory.getLogger(KafkaStreamManagerImpl.class);
    private static final String rootPath = "/kafkaStream/application/";
    private static ZooKeeper zooKeeper;
    public KafkaStreamManagerImpl(){
        zooKeeper = createZookeeper();
    }

    @Override
    public void startApplication(KafkaStreamApplication application) {
        if (!application.isAlive()){
            //启动成功后，设置状态并通知zookeeper
            KafkaStreamApplicationStat stat = KafkaStreamApplicationStat.STOP;
            try {
                application.start();
                stat = KafkaStreamApplicationStat.START;
            } catch (IllegalStateException | StreamsException e) {
                e.printStackTrace();
            }
            ZkUtils.setData(getZooKeeper(),rootPath+application.getApplicationName(),
                    stat.toString().getBytes(),-1);
        }
    }

    @Override
    public void stopApplication(KafkaStreamApplication application) {
        //停止成功后，通知zookeeper
        if (application.isAlive()){
            application.stop();
            ZkUtils.setData(getZooKeeper(),rootPath+application.getApplicationName(),KafkaStreamApplicationStat.STOP.toString().getBytes(),-1);
        }
    }

    @Override
    public void registerKafkaStreamApplication(KafkaStreamApplication application) {
        KafkaStream annotation = application.getClass().getAnnotation(KafkaStream.class);
        KafkaStreamApplicationStat stat = KafkaStreamApplicationStat.STOP;
        if (annotation.stat()==KafkaStreamApplicationStat.START) {
            try {
                application.start();
                stat = KafkaStreamApplicationStat.START;
            } catch (IllegalStateException|StreamsException e) {
                LOGGER.error("启动kafkaStreamApplication失败,error:{}",e);
            }
        }
        ZkUtils.createPath(getZooKeeper(), rootPath + application.getApplicationName(), stat.toString());
    }

    @Override
    public void watchKafkaStreamApplication(KafkaStreamApplication application) {
        ZkUtils.getData(getZooKeeper(),rootPath + application.getApplicationName(),
                new KafkaStreamWatcher(this,application),null);
    }

    /**
    *@ClassName KafkaStreamManagerImpl
    *@Author xiaoyu
    *@Date 2020/5/19 11:00
    *@Description TODO
    **/
    private static class KafkaStreamWatcher implements Watcher{
        private KafkaStreamApplication application;
        private KafkaStreamManager kafkaStreamManager;
        public KafkaStreamWatcher(KafkaStreamManager kafkaStreamManager,KafkaStreamApplication application){
            this.application = application;
            this.kafkaStreamManager = kafkaStreamManager;
        }

        @Override
        public void process(WatchedEvent event) {
                byte[] data = ZkUtils.getData(getZooKeeper(), rootPath + application.getApplicationName(), this, null);
                KafkaStreamApplicationStat stat = KafkaStreamApplicationStat.valueOf(new String(data));
                switch (stat){
                    case STOP:
                        kafkaStreamManager.stopApplication(application);
                        break;
                    case START:
                        kafkaStreamManager.startApplication(application);
                        break;
                    default:
                        break;
                }
        }
    }

    /**
     * 扫描一遍所有kafkaStreamApplication是否
     * @param apps
     */
    @Override
    public void supervisorKafkaStreamApps(Collection<KafkaStreamApplication> apps){
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (apps!=null && apps.size()>0){
                    for (KafkaStreamApplication app:apps){
                        if(!app.isAlive()){
                            ZkUtils.setData(getZooKeeper(),rootPath+app.getApplicationName(),KafkaStreamApplicationStat.STOP.toString().getBytes(),-1);
                        }
                    }
                }
            }
        },3000,60*1000);
    }

    /**
     * @return 创建zookeeper
     */
    private static ZooKeeper createZookeeper(){
        ZooKeeper zooKeeper = null;
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            Properties properties = PropertiesLoaderUtils.loadAllProperties("application.properties");
            zooKeeper = new ZooKeeper(properties.getProperty("zookeeper.url"), 300000, event -> {
                if (event.getState()== Watcher.Event.KeeperState.SyncConnected){
                    countDownLatch.countDown();
                }
            });
            countDownLatch.await();
        } catch (IOException | InterruptedException e) {
            LOGGER.error("创建zookeeper客户端失败",e);
        }
        return zooKeeper;
    }

    /**
     * 判断zookeeper是否可用，若不可用则创建一个
     * @return
     */
    private static ZooKeeper getZooKeeper(){
        if (zooKeeper==null || !zooKeeper.getState().isAlive()){
            synchronized (KafkaStreamManagerImpl.class){
                if (zooKeeper==null || !zooKeeper.getState().isAlive()){
                    zooKeeper = createZookeeper();
                }
            }
        }
        return zooKeeper;
    }
}
