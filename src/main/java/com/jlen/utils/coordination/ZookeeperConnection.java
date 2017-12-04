package com.jlen.utils.coordination;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

public interface ZookeeperConnection extends Watcher {
    
    LeaderElectionAccess getZkpLeaderElectionAccess(String serviceName) throws KeeperException, InterruptedException;
    
    static ZookeeperConnection getConnection(ZookeeperConfig config) throws IOException {
        return ZookeeperConnectionImpl.getConnection(config);
    }

}
