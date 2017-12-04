package com.jlen.utils.coordination;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

public interface ZookeeperPooledConnection extends Watcher {
    
    LeaderElectionAccess getZkpLeaderElectionAccess(String serviceName) throws KeeperException, InterruptedException;
    
    void destroy();
    
    static ZookeeperPooledConnection getConnection(ZookeeperConfig config) throws IOException {
        return ZookeeperPooledConnectionImpl.getConnection(config);
    }

}
