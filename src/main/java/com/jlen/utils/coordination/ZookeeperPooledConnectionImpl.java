package com.jlen.utils.coordination;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ZookeeperPooledConnectionImpl implements ZookeeperPooledConnection {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperPooledConnectionImpl.class);
    
    private static final Map<String, ZookeeperPooledConnectionImpl> cache = Maps.newConcurrentMap();
    private static final List<ZookeeperAccess> accessCache = Lists.newCopyOnWriteArrayList();
    
    private final ZooKeeper zookeeper;
    
    private final Lock statusNotifLock = new ReentrantLock();
    private final Condition statusChangeCondition = statusNotifLock.newCondition();
    
    private ZookeeperPooledConnectionImpl(ZookeeperConfig config) throws IOException {
        
        this.zookeeper = new ZooKeeper(config.getUrl(), config.getTimeoutMillis(), this);
        
        try {
            awaitConnected(config.getTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new IOException("Unable to connect to zookeeper", e);
        }
    }
    
    public static ZookeeperPooledConnection getConnection(ZookeeperConfig config) throws IOException {
        try {
            return cache.computeIfAbsent(config.getUrl(), u -> createNewConnection(config));
        } catch (Exception e) {
            throw new IOException("Unable to create new zookeeper connection", e.getCause());
        }
    }
    
    private static ZookeeperPooledConnectionImpl createNewConnection(ZookeeperConfig config) {
        try {
            return new ZookeeperPooledConnectionImpl(config);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to create zookeeper connection", e);
        }
    }
    
    private void awaitConnected(int timeout, TimeUnit unit) throws InterruptedException {
        
        Stopwatch timer = Stopwatch.createStarted();
        while(!zookeeper.getState().isConnected() || timer.elapsed(unit) <= timeout) {
            awaitCondition(statusNotifLock, statusChangeCondition, timeout, unit);
        }
        timer.stop();
        
    }
    
    @Override
    public void process(WatchedEvent state) {
        switch (state.getState()) {
        case SyncConnected:
            signalCondition(statusNotifLock, statusChangeCondition);
            break;
        default:
            break;
        }
    }
    
    private void signalCondition(Lock lock, Condition condition) {
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
    
    private void awaitCondition(Lock lock, Condition condition, long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            condition.await(timeout, unit);
        } finally {
            lock.unlock();
        }
    }
    
    @SuppressWarnings("unused")
    private void awaitCondition(Lock lock, Condition condition) throws InterruptedException {
        lock.lock();
        try {
            condition.await();
        } finally {
            lock.unlock();
        }
    }
    
    public LeaderElectionAccess getZkpLeaderElectionAccess(String root) throws KeeperException, InterruptedException {
        LeaderElectionAccess access = new LeaderElectionAccess(this.zookeeper, root);
        accessCache.add(access);
        return access;
    }

    @Override
    public void destroy() {
        cache.forEach((n, c) -> {
            try {
                c.zookeeper.close();
            } catch (InterruptedException e) {
                logger.warn("Unable to close zookeeper connection", e);
                Thread.currentThread().interrupt();
            }
        });
        accessCache.forEach(a -> {
            try {
                a.onDisconnect();
            } catch (Exception e) {
                logger.warn("Unable to invoke callback on zookeeper access", e);
            }
        });
    }

}
