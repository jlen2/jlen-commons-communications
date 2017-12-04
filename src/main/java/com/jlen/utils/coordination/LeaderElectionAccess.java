package com.jlen.utils.coordination;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class LeaderElectionAccess extends ZookeeperAccess {
    
    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionAccess.class);
    
    private final List<Consumer<State>> subscribers = Lists.newCopyOnWriteArrayList();
    private State state;
    
    private String thisNode;
    
    public static enum State {
        LEADER,
        SLAVE,
        ERROR
    }
    
    LeaderElectionAccess(ZooKeeper zkp, String root) throws KeeperException, InterruptedException {
        super(zkp, root);
        thisNode = createEphermalNode();
        logger.info("Created ephermal node [{}]", thisNode);
        tryLeaderElection();
    }
    
    @Override
    protected String createEphermalNode() throws KeeperException, InterruptedException {
        return shortNodePath(super.createEphermalNode());
    }
    
    private void tryLeaderElection() throws KeeperException, InterruptedException {
        NavigableSet<String> childNodes = new TreeSet<>(getChildNodes());
        if(childNodes.isEmpty()) {
            throw new IllegalStateException("No child nodes found");
        }
        String prevNode = childNodes.lower(thisNode);
        if(prevNode == null) {
            logger.info("Current node is the leader");
            state = State.LEADER;
        } else {
            setWatch(prevNode, this::handleEvent);
            state = State.SLAVE;
        }
    }

    private void handleEvent(WatchedEvent event) {
        if(EventType.NodeDeleted.equals(event.getType())) {
            try {
                tryLeaderElection();
                if(isLeader()) {
                    notifySubscribers();
                }
            } catch (KeeperException | InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private void notifySubscribers() {
        Iterator<Consumer<State>> iter = subscribers.iterator();
        while(iter.hasNext()) {
            iter.next().accept(state);
        }
    }

    public boolean isLeader() {
        return State.LEADER.equals(state);
    }
    
    public void subscribe(Consumer<State> fn) {
        subscribers.add(fn);
    }

    @Override
    protected void onDisconnect() {
        state = State.ERROR;
        notifySubscribers();
    }
    
}
