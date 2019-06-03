/**
 * 
 */
package com.example.zkleaderelection.ls;

import com.example.zkleaderelection.ls.LeaderElectionManager.EventType;

/**
 * @author dshekhar
 *
 */
public interface LeaderElectionAware {
	
	/**
	 * 
	 * @param eventType
	 */
	public void onElectionEvent(EventType eventType);

}
