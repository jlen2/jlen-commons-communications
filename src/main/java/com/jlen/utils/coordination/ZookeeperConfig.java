package com.jlen.utils.coordination;

import com.typesafe.config.Config;

public class ZookeeperConfig {
    
    private static final String PROP_URL = "url";
    private static final String PROP_TIMEOUT = "timeout.ms";
    
    private String url;
    private int timeout;
    
    public ZookeeperConfig(Config rootConfig) {
        this.url = rootConfig.getString(PROP_URL);
        this.timeout = rootConfig.getInt(PROP_TIMEOUT);
    }
    
    public String getUrl() {
        return this.url;
    }
    
    public int getTimeoutMillis() {
        return this.timeout;
    }

}
