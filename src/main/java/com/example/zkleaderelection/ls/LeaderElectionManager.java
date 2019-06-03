/**
 * 
 */
package com.example.zkleaderelection.ls;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dshekhar
 *
 */
public class LeaderElectionManager implements Watcher {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionManager.class.getName());

	private ZooKeeper zookeeper;
	private State state;
	private Set<LeaderElectionAware> listeners;

	private String rootNodeName;
	private String hostname;
	private LeaderOffer leaderOffer;

	public LeaderElectionManager() {
		state = State.STOP;
		listeners = Collections.synchronizedSet(new HashSet<LeaderElectionAware>());
	}

	public synchronized void start() {
		state = State.START;
		dispatchEvent(EventType.START);
		LOG.info("Starting Leader Election");

		if (null == zookeeper) {
			throw new IllegalStateException("No instance of the zookeeper provided. use setZookeeper().");
		}

		if (null == hostname) {
			throw new IllegalStateException("No hostname provided. use setHostname().");
		}

		try {
			makeOffer();
			determineElectionStatus();
		} catch (KeeperException e) {
			becomeFailed(e);
			return;
		} catch (InterruptedException e) {
			becomeFailed(e);
			return;
		}
	}

	public synchronized void stop() {
		state = State.STOP;
		dispatchEvent(EventType.STOP_START);
		LOG.info("Stopping leader election support");
		LOG.info("Removed leader offer {}", leaderOffer.getNodePath());

		if (null != leaderOffer) {
			try {
				zookeeper.delete(leaderOffer.getNodePath(), -1);
			} catch (InterruptedException e) {
				becomeFailed(e);
			} catch (KeeperException e) {
				becomeFailed(e);
			}
		}
		dispatchEvent(EventType.STOP_COMPLETE);
	}

	/**
	 * offer to create leader node to identify itself as leader.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private void makeOffer() throws KeeperException, InterruptedException {
		state = State.OFFER;
		dispatchEvent(EventType.OFFER_START);

		LeaderOffer newLeaderOffer = new LeaderOffer();
		byte[] hostnameBytes;

		synchronized (this) {
			newLeaderOffer.setHostName(hostname);
			hostnameBytes = hostname.getBytes();
			createRootIfNotExists();
			newLeaderOffer.setNodePath(zookeeper.create(rootNodeName + "/" + "n_", hostnameBytes,
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));
			leaderOffer = newLeaderOffer;
			LOG.debug("Created leader offer {}", leaderOffer);
			dispatchEvent(EventType.OFFER_COMPLETE);
		}
	}

	private void determineElectionStatus() throws KeeperException, InterruptedException {
		state = State.DETERMINE;
		dispatchEvent(EventType.DETERMINE_START);
		LeaderOffer currentLeaderOffer = getLeaderOffer();
		String[] components = currentLeaderOffer.getNodePath().split("/");
		currentLeaderOffer.setId(Integer.valueOf(components[components.length - 1].substring("n_".length())));

		List<LeaderOffer> leaderOffers = toLeaderOffers(zookeeper.getChildren(rootNodeName, false));
		for (int i = 0; i < leaderOffers.size(); i++) {
			LeaderOffer leaderOffer = leaderOffers.get(i);
			if (leaderOffer.getId().equals(currentLeaderOffer.getId())) {
				LOG.debug("There are {} leader offers. I am {} in line.", leaderOffers.size(), i);

				dispatchEvent(EventType.DETERMINE_COMPLETE);

				if (i == 0) {
					becomeLeader();
				} else {
					becomeReady(leaderOffers.get(i - 1));
				}
				break;
			}
		}
	}
	
	public void createRootIfNotExists() throws InterruptedException, KeeperException {
        Stat stat = zookeeper.exists(this.getRootNodeName(), false);
        if (stat == null) {
            try {
            	zookeeper.create(this.getRootNodeName(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ex) {
               LOG.error(ex.getMessage(), ex);
            }
        }
    }

	private void becomeReady(LeaderOffer neighborLeaderOffer) throws KeeperException, InterruptedException {
		LOG.info("{} not elected leader. Watching node:{}", getLeaderOffer().getNodePath(),
				neighborLeaderOffer.getNodePath());

		Stat stat = zookeeper.exists(neighborLeaderOffer.getNodePath(), this);

		if (null != stat) {
			dispatchEvent(EventType.READY_START);
			LOG.debug("We're behind {} in line and they're alive.", neighborLeaderOffer.getNodePath());
			state = State.READY;
			dispatchEvent(EventType.READY_COMPLETE);
		} else {
			LOG.info("We were behind {} but it looks like they died. Back to determination.",
					neighborLeaderOffer.getNodePath());
		}
	}

	private void becomeLeader() {
		state = State.ELECTED;
		dispatchEvent(EventType.ELECTED_START);

		LOG.info("Becoming leader with node:{}", getLeaderOffer().getNodePath());

		dispatchEvent(EventType.ELECTED_COMPLETE);
	}

	/**
	 * watch till node deleted event has occurred.
	 */
	@Override
	public void process(WatchedEvent event) {
		if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
			if (!event.getPath().equals(getLeaderOffer().getNodePath()) && state != State.STOP) {
				LOG.debug("Node {} deleted. Need to run through the election process.", event.getPath());
				try {
					determineElectionStatus();
				} catch (KeeperException e) {
					becomeFailed(e);
				} catch (InterruptedException e) {
					becomeFailed(e);
				}
			}
		}

	}

	public void dispatchEvent(EventType eventType) {
		LOG.debug("Dispatching event : {}", eventType);
		synchronized (listeners) {
			if (listeners.size() > 0) {
				listeners.forEach(p -> p.onElectionEvent(eventType));
			}
		}

	}

	private List<LeaderOffer> toLeaderOffers(List<String> strings) throws KeeperException, InterruptedException {

		List<LeaderOffer> leaderOffers = new ArrayList<LeaderOffer>(strings.size());

		for (String offer : strings) {
			String hostName = new String(zookeeper.getData(rootNodeName + "/" + offer, false, null));

			leaderOffers.add(new LeaderOffer(Integer.valueOf(offer.substring("n_".length())),
					rootNodeName + "/" + offer, hostName));
		}
		Collections.sort(leaderOffers, new LeaderOffer.IdComparator());

		return leaderOffers;
	}

	private void becomeFailed(Exception e) {
		LOG.error("Failed in state {} - Exception:{}", state, e);
		state = State.FAILED;
		dispatchEvent(EventType.FAILED);
	}

	public String getRootNodeName() {
		return rootNodeName;
	}

	public void setRootNodeName(String rootNodeName) {
		this.rootNodeName = rootNodeName;
	}

	public ZooKeeper getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(ZooKeeper zookeeper) {
		this.zookeeper = zookeeper;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public synchronized LeaderOffer getLeaderOffer() {
		return leaderOffer;
	}

	public void addListener(LeaderElectionAware listener) {
		listeners.add(listener);
	}

	public void removeListener(LeaderElectionAware listener) {
		listeners.remove(listener);
	}

	@Override
	public String toString() {
		return "{ state:" + state + " leaderOffer:" + getLeaderOffer() + " zooKeeper:" + zookeeper + " hostName:"
				+ getHostname() + " listeners:" + listeners + " }";
	}

	public static enum State {
		START, OFFER, DETERMINE, ELECTED, READY, FAILED, STOP
	}

	/**
	 * 
	 * @author dshekhar
	 *
	 */
	public static enum EventType {
		START, OFFER_START, OFFER_COMPLETE, DETERMINE_START, DETERMINE_COMPLETE, ELECTED_START, ELECTED_COMPLETE, READY_START, READY_COMPLETE, FAILED, STOP_START, STOP_COMPLETE
	}

}
