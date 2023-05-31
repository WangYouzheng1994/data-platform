package org.raise.cdc.oracle.connector;

import lombok.extern.slf4j.Slf4j;
import org.raise.cdc.oracle.config.OracleConnectorConfig;
import org.raise.cdc.oracle.config.OracleTaskConfig;
import org.raise.cdc.oracle.mapper.SqlUtil;

import java.sql.*;

/**
 * @Description: 核心处理类，线程（JDBCConnect也是一个链接）
 * @Author: WangYouzheng
 * @Date: 2023/5/31 15:24
 * @Version: V1.0
 */
@Slf4j
public class OracleConnector {
    private OracleConnectorContext connContext;

    private Connection connection;

    /**
     * 初始化线程
     */
    void init(OracleConnectorConfig config) {
        getConnection(config);
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
    }/*

    public BigInteger getStartSCN(OracleCDCConnection oracleCDCConnecUtil, BigInteger startScn) {
        Connection connection = oracleCDCConnecUtil.getConnection();

        // 如果从保存点模式开始 并且不是0 证明保存点是ok的
        if (startScn != null && startScn.compareTo(BigInteger.ZERO) != 0) {
            return startScn;
        }

        // 恢复位置为0，则根据配置项进行处理
        if (SCNReadType.ALL.name().equalsIgnoreCase(oracleCDCConfig.getReadPosition())) {
            // 获取最开始的scn
            startScn = oracleCDCConnecUtil.getMinScn(connection);
        } else if (SCNReadType.CURRENT.name().equalsIgnoreCase(oracleCDCConfig.getReadPosition())) {
            startScn = oracleCDCConnecUtil.getCurrentScn(connection);
        } else if (SCNReadType.TIME.name().equalsIgnoreCase(oracleCDCConfig.getReadPosition())) {
            // 根据指定的时间获取对应时间段的日志文件的起始位置
            if (oracleCDCConfig.getStartTime() == 0) {
                throw new IllegalArgumentException("[startTime] must not be null or empty when readMode is [time]");
            }
            startScn = oracleCDCConnecUtil.getLogFileStartPositionByTime(connection, oracleCDCConfig.getStartTime());
        } else if (SCNReadType.SCN.name().equalsIgnoreCase(oracleCDCConfig.getReadPosition())) {
            // 根据指定的scn获取对应日志文件的起始位置
            if (StringUtils.isEmpty(oracleCDCConfig.getStartSCN())) {
                throw new IllegalArgumentException("[startSCN] must not be null or empty when readMode is [scn]");
            }
            startScn = new BigInteger(oracleCDCConfig.getStartSCN());
        } else {
            throw new IllegalArgumentException(
                    "unsupported readMode : " + oracleCDCConfig.getReadPosition());
        }
        return startScn;
    }*/
}
