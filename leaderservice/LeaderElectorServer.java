package com.infinera.nm.ems.server.messaging.leaderservice;


import com.infinera.nm.common.utils.system.IConstants;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class LeaderElectorServer {

    public static Map<String, LeaderNodes> LeaderMap = new HashMap<String, LeaderNodes>();

	/*
		creating Leader latch and registering path in zookeeper to contend for leadership election with onother one 
		who registered in same path
	*/
    public static void createLeaderLatch(String leaderkey) {

        try {
            LeaderNodes leadernodes = null;
            LOGGER.severe("Leader key creating with : " + leaderkey);
            leadernodes = new LeaderNodes(leaderkey);
            LeaderMap.put(leaderkey, leadernodes);
            Thread.sleep(5000);
            LOGGER.severe("Leader key created with : " + leaderkey);

        } catch (Exception ee) {
            LOGGER.severe("Exceptiom :  Leader Elector");
        }
    }

	/*
	Closing of leader latch 
		will remove path from zookeeper and allows next one to contend for leadership who ever registered in same path
	*/
    public static void closeLeaderNodes(String leaderkey) throws Exception {
        LOGGER.severe("******************************************************************************************");
        if (LeaderMap.containsKey(leaderkey)) {

            LeaderNodes leadernodes = LeaderMap.get(leaderkey);
            LOGGER.severe(leaderkey + " to be removed for  : " + leadernodes);
            leadernodes.close();
            LeaderMap.remove(leaderkey);
            LOGGER.severe(leaderkey + " removed successfully");
        } else {

            LOGGER.severe(leaderkey + " not present");
        }
        LOGGER.severe("************************************************************************************************");
    }
	/*
	checks wether curator is available/up
	*/
	private static boolean isCuratorFrameworkAvailable(LeaderNodes leadernodes) throws Exception
    {
        boolean cf=false;
        int i=0;
        while(!leadernodes.checkCuratorFramework())
        {
            Thread.sleep(1000);
            //Just wait to get the leader
            if(i>MAXRETRY)
            {
                cf=true;
                break;
            }
            i++;
        }

        return cf;
    }

/*
		This will close all leader latch whe an application shuts down
*/
    final static class ShutdownHook implements Runnable {

        public void run() {
            try {
                for (String leaderkey : LeaderMap.keySet()) {
                    closeLeaderNodes(leaderkey);
                }
                LeaderNodeStatusListener.getInstance().unsubscribeForDiscoveryEvents();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }

        }
    }

}
