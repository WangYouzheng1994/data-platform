package org.raise.cdc.base.util;

import com.alibaba.druid.pool.DruidDataSource;
import org.raise.cdc.oracle.config.OracleTaskConfig;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

/**
 * @Description: 数据源创建工具
 * @Author: WangYouzheng
 * @Date: 2023/6/1 13:42
 * @Version: V1.0
 */
public class DataSourceUtil {


    /**
     * 获取德鲁伊数据连接池
     *
     * @param taskConfig
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static DataSource getDataSource(OracleTaskConfig taskConfig)
            throws SQLException, ClassNotFoundException {
        // Class.forName(driverName);

        String jdbcUrl = taskConfig.getJdbcUrl();
        String username = taskConfig.getUsername();
        String password = taskConfig.getPassword();
        String driverName = taskConfig.getDriverClass();
        String taskName = taskConfig.getTaskName();

        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(driverName);
        dataSource.setName(taskName + "-druid.source");
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(30);
        dataSource.setMaxWait(30000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setValidationQuery("select 'x' from dual");
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        dataSource.setKeepAlive(true);
        dataSource.setPoolPreparedStatements(false);
        dataSource.setConnectionInitSqls(Collections.singletonList("set names 'utf8'"));

        dataSource.setRemoveAbandoned(false);
        dataSource.setLogAbandoned(true);
        dataSource.setTimeBetweenConnectErrorMillis(60000);
        dataSource.setConnectionErrorRetryAttempts(3);

        dataSource.init();
        return dataSource;
    }
}
