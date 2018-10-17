package dynamicServer;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class dynamicServer {
    private static final String connectString = "192.168.48.132:2181,192.168.48.134:2181,192.168.48.135:2181";
    private static final int sessionTimeout = 20000;
    private static final String parentNode = "/servers";

    private ZooKeeper zkClient = null;
    static final CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public void getConnect() throws Exception{
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
                        //重新更新服务器列表，并且注册了监听
                        try {
                            zkClient.getChildren("/", true);
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
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
     * 向zk集群注册服务器信息
     *
     * @param hostname
     * @throws Exception
     */
    public void registerServer(String hostname) throws Exception {
        //参数4：CreateMode.EPHEMERAL_SEQUENTIAL表示使用临时节点
        String create = zkClient.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online.." + create);
    }

    /**
     * 业务功能
     *
     * @throws InterruptedException
     */
    public void handleBussiness(String hostname) throws InterruptedException {
        System.out.println(hostname + " start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {

        // 获取zk连接
        dynamicServer server = new dynamicServer();
        server.getConnect();

        // 利用zk连接注册服务器信息
        server.registerServer(args[0]);

        // 启动业务功能
        server.handleBussiness(args[0]);

    }

}
