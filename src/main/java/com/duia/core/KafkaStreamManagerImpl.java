package com.duia.core;

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
    public boolean startApplication(KafkaStreamApplication application) {
        if (application.getStat()== KafkaStreamApplication.KafkaStreamApplicationStat.STOP){
            application.start();
            //启动成功后，设置状态并通知zookeeper
            KafkaStreamApplicationStat stat = KafkaStreamApplicationStat.START;
            try {
                getZooKeeper().setData(rootPath+application.getApplicationName(),stat.toString().getBytes(),-1);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public boolean stopApplication(KafkaStreamApplication application) {
        if (application.getStat()== KafkaStreamApplication.KafkaStreamApplicationStat.START){
            application.stop();
            //启动成功后，通知zookeeper
            KafkaStreamApplicationStat stat = KafkaStreamApplicationStat.STOP;
            try {
                getZooKeeper().setData(rootPath+application.getApplicationName(),stat.toString().getBytes(),-1);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public boolean registerKafkaStreamApplication(KafkaStreamApplication application) {
        try {
            createPath(rootPath+application.getApplicationName(),application.getStat().toString());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void watchKafkaStreamApplication(KafkaStreamApplication application) {
        try {
            getZooKeeper().getData(rootPath + application.getApplicationName(),
                    new KafkaStreamWatcher(this,application),
                    null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
            byte[] data = new byte[0];
            try {
                    data = getZooKeeper().getData(rootPath + application.getApplicationName(), this, null);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String value = new String(data);
            KafkaStreamApplication.KafkaStreamApplicationStat stat = KafkaStreamApplication.KafkaStreamApplicationStat.valueOf(value);

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
     * zookeeper创建节点，若父节点不存在则先创建父节点 /a/b/c
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void createPath(String path,Object data) throws KeeperException, InterruptedException {
        List<String> newPaths = new ArrayList<>();
        char[] chars = path.toCharArray();
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i]=='/'){
                String newPath = stringBuffer.toString();
                if (newPath!=null && newPath.trim().length()>0){
                    newPaths.add(newPath);
                }
                stringBuffer = new StringBuffer();
            }
            stringBuffer.append(chars[i]);
        }

        if (stringBuffer.toString()!=null && stringBuffer.toString().trim().length()>0){
            newPaths.add(stringBuffer.toString());
        }

        for (int i = 0; i < newPaths.size(); i++) {
            List<String> newPath = newPaths.subList(0, i+1);
            StringBuffer buffer = new StringBuffer();
            for (String s:newPath){
                buffer.append(s);
            }
            byte[] d = (i==newPaths.size()-1)?data.toString().getBytes():null;
            if (zooKeeper.exists(buffer.toString(),null)==null){
                zooKeeper.delete(buffer.toString(),-1);
                zooKeeper.create(buffer.toString(),
                        d,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }

    /**
     * @return 创建zookeeper
     */
    private static ZooKeeper createZookeeper(){
        ZooKeeper zooKeeper = null;
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            Properties properties = PropertiesLoaderUtils.loadAllProperties("application.properties");
            zooKeeper = new ZooKeeper(properties.getProperty("zookeeper.url"), 3000, event -> {
                if (event.getState()== Watcher.Event.KeeperState.SyncConnected){
                    countDownLatch.countDown();
                }
            });
            countDownLatch.await();
        } catch (IOException | InterruptedException e) {
            LOGGER.error("创建zookeeper客户端失败",e.getMessage());
        }
        return zooKeeper;
    }

    /**
     * 判断zookeeper是否可用，若不可用则创建一个
     * @return
     */
    private static ZooKeeper getZooKeeper(){
        if (zooKeeper==null || zooKeeper.getState().isAlive()){
            synchronized (KafkaStreamManagerImpl.class){
                if (zooKeeper==null || zooKeeper.getState().isAlive()){
                    zooKeeper = createZookeeper();
                }
            }
        }
        return zooKeeper;
    }
}
