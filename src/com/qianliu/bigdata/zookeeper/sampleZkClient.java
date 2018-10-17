package com.qianliu.bigdata.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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
        System.out.println("....");
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
        String nodeCreated = zkClient.create("/eclipse", "hellozk4".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //上传的数据可以是任何类型，但都要转成byte[]
        System.out.println("创建节点"+nodeCreated);

    }

    // 获取子节点
    @Test
    public void getChildren() throws Exception {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
    }

    //判断节点是否存在
    @Test
    public void testExists() throws Exception{
        /*
         * @return 将"/eclispe"节点的所有信息返回，并且封装在stat中
        * */
        Stat stat = zkClient.exists("/eclipse",false);//判断"/eclispe"节点是否存在
        if(stat == null) {
            System.out.println("/eclipse不存在！");
        }else{
            System.out.println("/eclipse节点长度：" + stat.getDataLength());
        }
    }

    //获取某个节点的信息
    @Test
    public void getData() throws Exception{
        //stat的参数为null表示不去描述这个"/eclipse"的节点
        // （"/eclipse"节点的可能换过几次版本，此时是哪个版本，null不去描述就是获取最新版本的"/eclipse"节点）
        byte[] bytes =zkClient.getData("/eclipse",false,null);
        System.out.println("/eclipse节点中字符串："+new String(bytes));
    }

    //删除节点
    @Test
    public void deleteData() throws Exception{
        //第二个参数：version = -1表示删除所有版本
        zkClient.delete("/eclipse",-1);
        System.out.println("/eclipse节点被删除！");
    }

    //设置某个节点的信息
    @Test
    public void setData() throws Exception{
        zkClient.setData("/eclipse","i miss you".getBytes(),-1);
        //System.out.println("/eclipse节点中字符串："+new String(bytes));
    }

    //结束以后必须关闭zookeeper连接
    @After
    public void closeZkConnection() throws Exception{
        zkClient.close();
    }
}
