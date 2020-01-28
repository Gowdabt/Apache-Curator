package com.infinera.nm.ems.server.messaging.leaderservice;

import com.infinera.nm.common.fault.AlarmSeverityUtil;
import com.infinera.nm.common.utils.NETypeCache;
import com.infinera.nm.common.utils.system.MoIdUtil;
import com.infinera.nm.ems.server.discovery.*;
import com.infinera.nm.ems.server.maps.TopologyAlertCache;
import com.infinera.nm.ems.server.maps.TopologyAlertInfo;
import com.infinera.nm.ems.server.mo.TopoNode;

import java.util.logging.Level;
import java.util.logging.Logger;

/*
    Harshith Gowda B T created on 14-Jan-20 
*/
public class LeaderOperation {


    public void createLeaderLatch(String id) {
        LeaderElectorServer.createLeaderLatch(id);
    }
	
	public boolean isLeader(String id){
		return LeaderElectorServer.LeaderMap.get(id).isLeader();
	}

    public void close leaderLatch(String id) {
        try {
            LeaderElectorServer.closeLeaderNodes(id);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "exception while closing leader latch as its nodeDeleted");
        }
    }


}
