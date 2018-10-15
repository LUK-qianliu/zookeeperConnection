package com.qianliu.bigdata.zookeeper;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class sampleZkClient {
    private static final String connectString = "192.168.48.132:2181,192.168.48.134:2181,192.168.48.135:2181";
    private static final int sessionTimeout = 2000;

    /** 信号量，阻塞程序执行，用于等待zookeeper连接成功，发送成功信号 */
    /*一旦不加锁，会因为连接zookeeper需要10s，而程序执行需要5s，故程序执行到向zookeeper节点写数据时
    ，zookeeper还没有连接上，因此程序而报错
    */
    static final CountDownLatch connectedSemaphore = new CountDownLatch(1);
    ZooKeeper zkClient = null;

    @Before
    public void testInit() throws Exception{
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 获取事件的状态
                Event.KeeperState keeperState = event.getState();
                Event.EventType eventType = event.getType();
                // 如果是建立连接
                if (Event.KeeperState.SyncConnected == keeperState) {
                    if (Event.EventType.None == eventType) {
                        // 如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
                        System.out.println("zk 建立连接");
                        connectedSemaphore.countDown();
                    }
                }
            }
        });

        // 进行阻塞
        connectedSemaphore.await();
        System.out.println("..");
    }
    /**
     * 数据的增删改查
     *
     * @throws InterruptedException
     * @throws KeeperException
     */

    // 创建数据节点到zk中
    @Test
    public void testCreate() throws KeeperException, InterruptedException {
        // 参数1：要创建的节点的路径 参数2：节点大数据 参数3：节点的权限 参数4：节点的类型
        String nodeCreated = zkClient.create("/eclipse4", "hellozk4".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //上传的数据可以是任何类型，但都要转成byte[]
        System.err.println(nodeCreated);

    }

    // 获取子节点
    @Test
    public void getChildren() throws Exception {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
        zkClient.close();
    }

    @After
    public void closeZkConnection() throws Exception{
        zkClient.close();
    }
}
