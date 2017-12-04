package com.jlen.utils.coordination;

import java.util.List;
import java.util.function.Consumer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ZookeeperAccess {
    
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperAccess.class);
    
    protected static final String PATH_SEP = "/";
    
    private final String root;
    protected final ZooKeeper zkp;
    
    protected ZookeeperAccess(ZooKeeper zkp, String root) {
        
        this.zkp = zkp;
        this.root = root.startsWith(PATH_SEP) ? root : PATH_SEP + root;
        try {
            createRoot();
        } catch (KeeperException e) {
            logger.warn("Unable to create root node ", e);
        } catch (InterruptedException e) {
            logger.warn("Unable to create root node ", e);
            Thread.currentThread().interrupt();
        }
        
        zkp.register(e -> {
            if(e.getState().equals(KeeperState.Disconnected)) {
                onDisconnect();
            }
        });
    }
    
    private void createRoot() throws KeeperException, InterruptedException {
        
        Stat stat = zkp.exists(root, false);
        if(stat == null) {
            zkp.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.debug("Root node [{}] created", root);
        } else {
            logger.debug("Root node [{}] already exists", root);
        }
    }
    
    protected String fullNodePath(String path) {
        StringBuilder sb = new StringBuilder(root);
        sb.append(PATH_SEP).append(path);
        return sb.toString();
    }
    
    protected String shortNodePath(String path) {
        return path.substring(path.lastIndexOf(PATH_SEP)+1);
    }
    
    protected String createEphermalNode() throws KeeperException, InterruptedException {
        return zkp.create(root+PATH_SEP, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    
    protected void createEphermalSeqNode(String path) throws KeeperException, InterruptedException {
        zkp.create(fullNodePath(path), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    
    protected List<String> getChildNodes() throws KeeperException, InterruptedException {
        return zkp.getChildren(root, false);
    }
    
    protected List<String> getChildNodes(String path) throws KeeperException, InterruptedException {
        return zkp.getChildren(fullNodePath(path), false);
    }
    
    protected boolean setWatch(String path, Consumer<WatchedEvent> fn) throws KeeperException, InterruptedException {
        logger.info("Setting watch on [{}]", path);
        return zkp.exists(fullNodePath(path), fn::accept) != null;
    }
    
    protected abstract void onDisconnect();

}
