package org.raise.cdc.oracle.connector;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.raise.cdc.base.config.BaseContextConfig;
import org.raise.cdc.base.config.ConnectorContext;
import org.raise.cdc.oracle.bean.OracleLogFile;
import org.raise.cdc.oracle.config.OracleConnectorConfig;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description: 当前Connecor连接
 * @Author: WangYouzheng
 * @Date: 2023/5/31 15:25
 * @Version: V1.0
 */
@Slf4j
@Data
public class OracleConnectorContext<T extends OracleConnectorConfig> implements ConnectorContext {
    // -------- JDBC Start
    private Connection connection;

    /**
     * 使用PLSQL，发起Logminer进行日志添加以及日志挖掘开启动作
     */
    @Deprecated
    private CallableStatement logMinerStartStmt;

    /**
     * 查询$Log_content的数据挖掘结果
     */
    @Deprecated
    private PreparedStatement logMinerSelectStmt;

    /**
     * logcontent结果集
     */
    @Deprecated
    private ResultSet logMinerData;

    T connectorConfig;

    /**
     * 当前线程加载到的日志文件
     */
    private List<OracleLogFile> addedOracleLogFiles = new ArrayList<>();

    /**
     * 任务上下文
     */
    private BaseContextConfig contextConfig;
}
