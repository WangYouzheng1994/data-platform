package org.raise.cdc.base.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.*;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/31 20:44
 * @Version: V1.0
 */
@Slf4j
public class JDBCConnector {
    protected Connection connection;
    protected CallableStatement callableStatement;
    protected PreparedStatement preparedStatement;

    /**
     * 发送jdbc请求，自带重试
     * @param sql
     */
    public void sendJdbc(String sql) {

    }

    /** 关闭数据库连接资源 */
    protected void closeResources(ResultSet rs, Statement stmt, Connection conn) {
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

    /**
     * 发送prepareStatement
     *
     * @param sql
     * @throws SQLException
     */
    protected void prepareStatement(String sql) throws SQLException {
        connection.prepareStatement(sql);
    }

    /**
     * 发送prepareCall
     *
     * @param sql
     * @throws SQLException
     */
    protected void prepareCall(String sql, String... params) throws SQLException {
        connection.prepareCall(sql);

        if (ArrayUtils.isNotEmpty(params)) {
            for (int i = 1; i <= params.length; i++) {
                callableStatement.setString(i, params[i]);
            }
        }

        callableStatement.execute();
    }

    /**
     * TODO: 重试发请求
     *
     * @param sql
     * @param retries
     * @throws SQLException
     */
    protected void prepareCall(String sql, Short retries) throws SQLException {
        boolean rst = false;
        do {
            prepareCall(sql);
        } while(!rst);
    }

    /** 关闭Statement */
    protected void closeStmt(Statement statement) {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        } catch (SQLException e) {
            log.warn("Close statement error", e);
        }
    }

}
