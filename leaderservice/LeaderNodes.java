package com.infinera.nm.ems.server.messaging.leaderservice;

import com.infinera.nm.common.utils.system.IConstants;
import com.infinera.nm.ems.server.messaging.kafkaservice.KafkaInternalTopicRouter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryForever;

import java.io.IOException;
import java.util.logging.Logger;

public class LeaderNodes {
    private String BaseLeaderPath = "/leader";
    private String leaderkey = null;
    private String leaderid = null;
    private static String zookeperHost = null;
    private static String zookeperPort = null;
    private CuratorFramework cf = null;
    private LeaderLatch leaderLatch;

    private static final Logger LOGGER = Logger.getLogger(LeaderNodes.class.getName());

    public LeaderNodes(String leaderkey) {
        this.leaderkey = BaseLeaderPath + "/" + leaderkey;
        initconfig();
        initCuratorInstance(leaderkey);

    }


    private void initconfig() {
        zookeperHost =  "localhost";
        zookeperPort =  "2181";
        leaderid = "Testid";
    }


    private void initCuratorInstance(String id) {
        try {
            cf = CuratorFrameworkFactory.newClient(zookeperHost + ":" + zookeperPort, new RetryForever(10000));
            cf.start();
            cf.getZookeeperClient().blockUntilConnectedOrTimedOut();
            initLeaderInstance(id);
        } catch (Exception curxption) {
            LOGGER.severe("Exception :: Could not able to create Curator Instance ");
        }
    }

    private void initLeaderInstance(String id) throws Exception {
        leaderLatch = new LeaderLatch(cf, this.leaderkey, this.leaderid);
        LeaderElectorServer.leaderIdMap.put(id, "false");
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                LeaderElectorServer.leaderIdMap.put(id, "true");
                LOGGER.severe("***************************** " + id + "  elected as leader ***********************************");
            
            }

            @Override
            public void notLeader() {
                LeaderElectorServer.leaderIdMap.put(id, "false");
                LOGGER.severe("################################ " + id + "  is not a leader ##################################");
            }
        });
        leaderLatch.start();
    }

	public boolean isLeader() throws Exception{
        return leaderLatch.hasLeadership();
    }

    public void close() throws IOException {
        leaderLatch.close();
        cf.close();
    }

    public void closeCurator() throws IOException {
        cf.close();
    }


}
