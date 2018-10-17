package dynamicServer;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static dynamicServer.dynamicServer.connectedSemaphore;

public class dynamicClient {
    private static final String connectString = "192.168.48.132:2181,192.168.48.134:2181,192.168.48.135:2181";
    private static final int sessionTimeout = 20000;
    private static final String parentNode = "/servers";
    // 注意:加volatile的意义何在？
    /* 多线程中，所有共享的代码在堆中，而每个线程（线程1，线程2，线程3）带代码在栈中执行
    *如果说线程1需要用堆中的某个数据A，他就会从堆中拿出代码A，复制一份A，执行完成操作以后再写会到堆中
    * 如果在这期间，有一个线程2进入到堆中去拿数据A，就会出现错误A被拿走的情况，
    * 加上volatile关键字以后就关于A的操作都是在堆中进行，没有了复制A的过程，这样每个线程的数据都是在正确的
     */
    private volatile List<String> serverList;
    private ZooKeeper zkClient = null;

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
                        connectedSemaphore.countDown();
                        //重新更新服务器列表，并且注册了监听
                        try {
                            getServerList();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });

        // 进行阻塞
        if(connectedSemaphore.getCount()>0){
            connectedSemaphore.await();
        }
        System.out.println("....");
    }

    /**
     * 获取服务器信息列表
     *
     * @throws Exception
     */
    public void getServerList() throws Exception {

        // 获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zkClient.getChildren(parentNode, true);

        // 先创建一个局部的list来存服务器信息
        List<String> servers = new ArrayList<String>();
        for (String child : children) {
            // child只是子节点的节点名
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        // 把servers赋值给成员变量serverList，已提供给各业务线程使用
        serverList = servers;

        //打印服务器列表
        System.out.println(serverList);
    }

    /**
     * 业务功能
     *
     * @throws InterruptedException
     */
    public void handleBussiness() throws InterruptedException {
        System.out.println("client start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }


    public static void main(String[] args) throws Exception {

        // 获取zk连接
        dynamicClient client = new dynamicClient();
        client.getConnect();
        // 获取servers的子节点信息（并监听），从中获取服务器信息列表
        client.getServerList();

        // 业务线程启动
        client.handleBussiness();
    }
}
