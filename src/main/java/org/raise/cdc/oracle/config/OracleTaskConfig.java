package org.raise.cdc.oracle.config;

import lombok.Builder;
import lombok.Data;
import org.raise.cdc.base.config.BaseStartConfig;
import org.raise.cdc.base.config.DataReadType;

import java.util.List;

/**
 * @Description: 开启一个CDC 任务的参数，此类需要考虑持久化到数据库，从数据库加载。
 * @Author: WangYouzheng
 * @Date: 2022/8/3 13:19
 * @Version: V1.0
 */
// @Data
@Builder
public class OracleTaskConfig extends BaseStartConfig {
    /**
     * 任务名称
     */
    private String taskName;

    // 默认是增量抽取：除此以外的模式为：all， time, scn
    private DataReadType readPosition = DataReadType.ALL;

    // 指定时间抽取  time模式
    private Long startTime;

    // 指定时间抽取  time模式
    private String endTime;

    // 指定SCN抽取  SCN模式
    private String startSCN;

    // 指定SCN结束  SCN模式
    private String endSCN;

    // 查询logminer 解析结果 v$logmnr_contents的数量
    private int fetchSize = 1000;

    // 要过滤的表 schema.TableName 大写
    private List<String> tables;

    //处理日志为相同SCN数据得情况，以及异常出错位置[1.正常记录 2.创建日志报错 3.获取试图报错  4.写入数据逻辑报错]
    private int identification;

    //V$LOGMNR_CONTENTS 的唯一值
    private String rs_id;

    /**
     * jdbc连接配置
     */
    private JdbcConfig jdbcConfig;

    /**
     * 全量快照并行度（线程数量）影响到各个任务执行的表个数
     */
    private Short snapShotParallelism;

    //创建builder方法，返回一个构建器
    public static OracleTaskConfigBuilder builder() {
        return new OracleTaskConfigBuilder();
    }

    /**
     * 数据库连接配置
     *
     * @return
     */
    public String getJdbcUrl() {
        return jdbcConfig.getJdbcUrl();
    }

    /**
     * 登录账号
     *
     * @return
     */
    public String getUsername() {
        return jdbcConfig.getUsername();
    }

    /**
     * 驱动的全路径类名
     *
     * @return
     */
    public String getDriverClass() {
        return jdbcConfig.getDriverClass();
    }

    /**
     * 获取密码
     *
     * @return
     */
    public String getPassword() {
        return jdbcConfig.getPassword();
    }

    /**
     * TaskConfigBuilder
     */
    public static class OracleTaskConfigBuilder {

        public OracleTaskConfigBuilder jdbcUrl(String jdbcUrl) {
            this.jdbcConfig.jdbcUrl = jdbcUrl;
            return this;
        }

        public OracleTaskConfigBuilder userName(String userName) {
            this.jdbcConfig.jdbcUrl = userName;
            return this;
        }

        public OracleTaskConfigBuilder password(String password) {
            this.jdbcConfig.password = password;
            return this;
        }

        public OracleTaskConfigBuilder driverClass(String driverClass) {
            this.jdbcConfig.driverClass = driverClass;
            return this;
        }
    }

    /**
     * jdbc配置
     */
    @Data
    public static class JdbcConfig {
        /**
         * source jdbc 驱动连接
         */
        private String jdbcUrl;

        /**
         * source jdbc username
         */
        private String username;

        /**
         * source jdbc password
         */
        private String password;

        /**
         * source jdbc 驱动类class
         */
        private String driverClass;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public DataReadType getReadPosition() {
        return readPosition;
    }

    public void setReadPosition(DataReadType readPosition) {
        this.readPosition = readPosition;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getStartSCN() {
        return startSCN;
    }

    public void setStartSCN(String startSCN) {
        this.startSCN = startSCN;
    }

    public String getEndSCN() {
        return endSCN;
    }

    public void setEndSCN(String endSCN) {
        this.endSCN = endSCN;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public int getIdentification() {
        return identification;
    }

    public void setIdentification(int identification) {
        this.identification = identification;
    }

    public String getRs_id() {
        return rs_id;
    }

    public void setRs_id(String rs_id) {
        this.rs_id = rs_id;
    }

    public JdbcConfig getJdbcConfig() {
        return jdbcConfig;
    }

    public void setJdbcConfig(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
    }

    public Short getSnapShotParallelism() {
        return snapShotParallelism;
    }

    public void setSnapShotParallelism(Short snapShotParallelism) {
        this.snapShotParallelism = snapShotParallelism;
    }
}
