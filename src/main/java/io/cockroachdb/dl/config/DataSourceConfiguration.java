package io.cockroachdb.dl.config;

import com.zaxxer.hikari.HikariDataSource;

import net.ttddyy.dsproxy.listener.logging.DefaultQueryLogEntryCreator;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.listener.logging.SLF4JQueryLoggingListener;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import javax.sql.DataSource;
import java.sql.Connection;

@Configuration
public class DataSourceConfiguration {
    public static final String SQL_TRACE_LOGGER = "io.cockroachdb.dl.SQL_TRACE";

    @Bean
    @Primary
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @Primary
    @Lazy
    public DataSource primaryDataSource() {
        LazyConnectionDataSourceProxy proxy = new LazyConnectionDataSourceProxy();
        proxy.setTargetDataSource(loggingProxy(targetDataSource()));
        proxy.setDefaultAutoCommit(true);
        proxy.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        return proxy;
    }

    private DataSource loggingProxy(DataSource dataSource) {
        DefaultQueryLogEntryCreator creator = new DefaultQueryLogEntryCreator();
        creator.setMultiline(false);

        SLF4JQueryLoggingListener listener = new SLF4JQueryLoggingListener();
        listener.setLogger(SQL_TRACE_LOGGER);
        listener.setLogLevel(SLF4JLogLevel.TRACE);
        listener.setQueryLogEntryCreator(creator);

        return ProxyDataSourceBuilder
                .create(dataSource)
                .name("SQL-Trace")
                .asJson()
                .listener(listener)
                .multiline()
                .build();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariDataSource targetDataSource() {
        HikariDataSource ds = dataSourceProperties()
                .initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
        ds.setAutoCommit(true);
        ds.addDataSourceProperty("ApplicationName", "dl");
        return ds;
    }
}
