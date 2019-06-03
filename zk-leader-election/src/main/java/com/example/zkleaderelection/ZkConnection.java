/**
 * 
 */
package com.example.zkleaderelection;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author dshekhar
 *
 */
public class ZkConnection {

	private static CountDownLatch connectionLatch = new CountDownLatch(1);

	public static ZooKeeper connect(String host) throws IOException, InterruptedException {
		ZooKeeper zookeeper = new ZooKeeper(
				host,
				2000,
				new Watcher() {
					public void process(WatchedEvent we) {
						if (we.getState() == KeeperState.SyncConnected) {
							connectionLatch.countDown();
						}
					}
				});
		connectionLatch.await();
		return zookeeper;
	}

	public static void close(ZooKeeper zookeeper) throws InterruptedException {
		if(null != zookeeper) {
			zookeeper.close();
		}
	}

}
