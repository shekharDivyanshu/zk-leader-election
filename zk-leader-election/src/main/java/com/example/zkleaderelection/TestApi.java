/**
 * 
 */
package com.example.zkleaderelection;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.example.zkleaderelection.ls.LeaderElectionManager;

/**
 * @author dshekhar
 *
 */
@RestController
public class TestApi {
	
	private static final Logger LOG = LoggerFactory.getLogger(TestApi.class.getName());
	
	private ZooKeeper zookeeper1;
	private ZooKeeper zookeeper2;
	private LeaderElectionManager node1ConnectionManager1;
	private LeaderElectionManager node1ConnectionManager2;
	
	@RequestMapping(value="/connect1", method=RequestMethod.GET)
	public void connect1() {
		try {
			zookeeper1  = ZkConnection.connect("localhost:2181");
			node1ConnectionManager1 = new LeaderElectionManager();
			node1ConnectionManager1.setHostname("node-1");
			node1ConnectionManager1.setZookeeper(zookeeper1);
			node1ConnectionManager1.setRootNodeName("/election");
			node1ConnectionManager1.start();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@RequestMapping(value="/connect2", method=RequestMethod.GET)
	public void connect2() {
		try {
			zookeeper2  = ZkConnection.connect("localhost:2181");
			
			node1ConnectionManager2 = new LeaderElectionManager();
			node1ConnectionManager2.setHostname("node-2");
			node1ConnectionManager2.setZookeeper(zookeeper2);
			node1ConnectionManager2.setRootNodeName("/election");
			node1ConnectionManager2.start();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@RequestMapping(value="/disconnect/{id}", method=RequestMethod.GET)
	public void disconnect(@PathVariable("id") int id) {
		if(id == 1) {
			if(null != node1ConnectionManager1) {
				node1ConnectionManager1.stop();
				LOG.info(" node1ConnectionManager1 stopped ...");
			}
			
		}else if (id == 2) {
			if(null != node1ConnectionManager1) {
				node1ConnectionManager2.stop();
				LOG.info(" node1ConnectionManager2 stopped ...");
			}
		}
	}
	
	@RequestMapping(value="/disconnect/zk/{id}", method=RequestMethod.GET)
	public void disconnectZookeeper(@PathVariable("id") int id) {
		try {
			if(id == 1) {
				if(null != zookeeper1) {
					ZkConnection.close(zookeeper1);
					LOG.info(" zookeeper1 disconnected ...");
				}
				
			}else if (id == 2) {
				if(null != zookeeper2) {
					ZkConnection.close(zookeeper2);
					LOG.info(" zookeeper2 disconnected ...");
				}
			}
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
