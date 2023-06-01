package org.raise.cdc.base.util;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/31 20:44
 * @Version: V1.0
 */
@Slf4j
public class JDBCConnector {
    private Connection connection;


    /**
     * 发送jdbc请求，自带重试
     * @param sql
     */
    public void sendJdbc(String sql) {

    }

    /** 关闭数据库连接资源 */
    public void closeResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.warn("Close resultSet error: {}", e.getMessage());
            }
        }
        if (null != stmt) {
            closeStmt(stmt);
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.warn("Close connection error:{}", e.getMessage());
            }
        }
    }

    /** 关闭Statement */
    private void closeStmt(Statement statement) {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        } catch (SQLException e) {
            log.warn("Close statement error", e);
        }
    }

}
