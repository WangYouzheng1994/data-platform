package org.raise.cdc.base.util;

import com.alibaba.druid.pool.DruidDataSource;
import org.raise.cdc.oracle.config.OracleTaskConfig;
import org.raise.cdc.oracle.mapper.SqlUtil;

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
        // 初始化连接数
        dataSource.setInitialSize(1);
        // 最小空闲
        dataSource.setMinIdle(1);
        // 最大活跃连接数
        dataSource.setMaxActive(30);
        // 获取连接时最大等待时间，单位毫秒
        dataSource.setMaxWait(30000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        // 设置空闲连接的最小可驱逐时间（毫秒
        dataSource.setMinEvictableIdleTimeMillis(300000);
        // 连接检测语句
        dataSource.setValidationQuery("select 'x' from dual");
        // 连接检测超时时间,秒
        dataSource.setValidationQueryTimeout(20);
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(true);
        dataSource.setTestOnReturn(false);
        dataSource.setKeepAlive(true);
        dataSource.setPoolPreparedStatements(false);
        // dataSource.setConnectionInitSqls(Collections.singletonList("set names 'utf8'"));
        // 获取连接的初始化
        dataSource.setConnectionInitSqls(Collections.singletonList(SqlUtil.SQL_ALTER_NLS_SESSION_PARAMETERS));
        // 设置出错重试次数
        dataSource.setConnectionErrorRetryAttempts(5);
        // 设置出错重试之间的间隔
        dataSource.setTimeBetweenConnectErrorMillis(1000);

        dataSource.setRemoveAbandoned(false);
        dataSource.setLogAbandoned(true);

        // 开启preparedstatement 缓存池 oracle提升较高。
        dataSource.setPoolPreparedStatements(true);
        // 此数值大于0 preparedstatement pool才会生效
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(100);

        // 查询超时，需要jdbc驱动支持
        dataSource.setQueryTimeout(600);
        // dataSource.setPhyTimeoutMillis();

        /*dataSource.setTimeBetweenConnectErrorMillis(60000);
        dataSource.setConnectionErrorRetryAttempts(3);*/

        dataSource.init();
        return dataSource;
    }
}
