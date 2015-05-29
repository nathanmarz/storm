package org.apache.storm.jdbc.common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class HikariCPConnectionProvider implements ConnectionProvider {

    private Map<String, Object> configMap;
    private transient HikariDataSource dataSource;

    public HikariCPConnectionProvider(Map<String, Object> hikariCPConfigMap) {
        this.configMap = hikariCPConfigMap;
    }

    @Override
    public synchronized void prepare() {
        if(dataSource == null) {
            Properties properties = new Properties();
            properties.putAll(configMap);
            HikariConfig config = new HikariConfig(properties);
            this.dataSource = new HikariDataSource(config);
            this.dataSource.setAutoCommit(false);
        }
    }

    @Override
    public Connection getConnection() {
        try {
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        if(dataSource != null) {
            dataSource.shutdown();
        }
    }
}
