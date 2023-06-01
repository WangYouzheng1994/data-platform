package org.raise.cdc.base.util;

import com.alibaba.druid.pool.DruidDataSource;

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
     * @param properties
     * @param driverName
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static DataSource getDataSource(Properties properties, String driverName)
            throws SQLException, ClassNotFoundException {
        Class.forName(driverName);

        String jdbcUrl = (String) properties.get(JDBC_URL_KEY);
        String username = (String) properties.get(USER_NAME_KEY);
        String password =
                properties.get(PASSWORD_KEY) == null ? null : (String) properties.get(PASSWORD_KEY);
        String database = (String) properties.get(DATABASE_KEY);

        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driverName);
        dataSource.setName(database + "-druid.source");

        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(5);
        dataSource.setMaxWait(30000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setValidationQuery("select 'x'");
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
