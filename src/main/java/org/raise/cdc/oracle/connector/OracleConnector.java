package org.raise.cdc.oracle.connector;

import lombok.extern.slf4j.Slf4j;
import org.raise.cdc.base.config.DataReadType;
import org.raise.cdc.base.util.JDBCConnector;
import org.raise.cdc.oracle.config.OracleConnectorConfig;
import org.raise.cdc.oracle.config.OracleTaskConfig;
import org.raise.cdc.oracle.constants.LogminerKeyConstants;
import org.raise.cdc.oracle.mapper.SqlUtil;

import java.math.BigInteger;
import java.sql.*;

/**
 * @Description: 核心处理类，线程（JDBCConnect也是一个链接）
 * @Author: WangYouzheng
 * @Date: 2023/5/31 15:24
 * @Version: V1.0
 */
@Slf4j
public class OracleConnector extends JDBCConnector {
    private OracleConnectorContext connContext;
    private Connection connection;

    /**
     * 初始化线程
     */
    void init(OracleConnectorConfig config) {
        // 初始化连接
        getConnection(config);
    }

    /**
     * 加载驱动，创建连接
     *
     * @param config
     * @return
     */
    public boolean getConnection(OracleTaskConfig config) {
        int interval = 1;
        log.debug("connection driver class: {}", config.getDriverClass());
        log.info("connection user: {}", config.getUsername());
        log.info("connection password: {}", config.getPassword());

        // 加载驱动
        try {
            Class.forName(config.getDriverClass());
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
            return false;
        }

        do {
            try {
                connection = DriverManager.getConnection(config.getJdbcUrl(), config.getUsername(), config.getPassword());
                interval = 5;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                closeResources(null, null, connection);
                interval++;
            }
        } while(interval < 3);

        interval = 1;
        // 设置编码和日期类型
        // 获取当前Oracle的字符集。
        do {
            try (PreparedStatement preparedStatement =
                         connection.prepareStatement(SqlUtil.SQL_ALTER_NLS_SESSION_PARAMETERS)) {
                // preparedStatement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
                preparedStatement.execute();
                interval = 5;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                closeResources(null, null, connection);
                interval++;
            }
        } while(interval < 3);

        boolean result = false;
        if (connection == null) {
            result = false;
        } else {
            result = true;
        }
        return result;
    }

    public BigInteger getStartSCN(BigInteger startScn) {
        // 如果从保存点模式开始 并且不是0 证明保存点是ok的
        if (startScn != null && startScn.compareTo(BigInteger.ZERO) != 0) {
            return startScn;
        }
        OracleConnectorConfig connectorConfig = connContext.getConnectorConfig();
        DataReadType dataReadType = connectorConfig.getDataReadType();

        // 恢复位置为0，则根据配置项进行处理
        if (DataReadType.ALL.equals(dataReadType)) {
            // 获取最开始的scn
            startScn = getMinScn(connection);
        } else if (DataReadType.CURRENT.equals(dataReadType)) {
            startScn = getCurrentScn(connection);
        } else if (DataReadType.TIME.name().equals(dataReadType)) {
            // 根据指定的时间获取对应时间段的日志文件的起始位置

        }/* else if (DataReadType.SCN.name().equalsIgnoreCase(oracleCDCConfig.getReadPosition())) {
            // 根据指定的scn获取对应日志文件的起始位置

        } */else {

        }
        return startScn;
    }

    /**
     * 获取最小SCN
     *
     * @param connection
     * @return
     */
    public BigInteger getMinScn(Connection connection) {
        BigInteger minScn = null;
        PreparedStatement minScnStmt = null;
        ResultSet minScnResultSet = null;

        try {
            minScnStmt = connection.prepareStatement(SqlUtil.SQL_GET_LOG_FILE_START_POSITION);

            minScnResultSet = minScnStmt.executeQuery();
            while (minScnResultSet.next()) {
                minScn = new BigInteger(minScnResultSet.getString(LogminerKeyConstants.KEY_FIRST_CHANGE));
            }

            return minScn;
        } catch (SQLException e) {
            log.error(" obtaining the starting position of the earliest archive log error", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(minScnResultSet, minScnStmt, null);
        }
    }

    /**
     * 获取当前SCN
     *
     * @param connection
     * @return
     */
    public BigInteger getCurrentScn(Connection connection) {
        BigInteger currentScn = null;
        PreparedStatement currentScnStmt = null;
        ResultSet currentScnResultSet = null;

        try {
            currentScnStmt = connection.prepareStatement(SqlUtil.SQL_GET_CURRENT_SCN);

            currentScnResultSet = currentScnStmt.executeQuery();
            while (currentScnResultSet.next()) {
                currentScn = new BigInteger(currentScnResultSet.getString(LogminerKeyConstants.KEY_CURRENT_SCN));
            }
            return currentScn;
        } catch (SQLException e) {
            log.error("获取当前的SCN出错:", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(currentScnResultSet, currentScnStmt, null);
        }
    }
}
