package org.raise.cdc.oracle.config;

import lombok.Builder;
import lombok.Data;
import org.raise.cdc.base.config.DataReadType;

import java.util.List;


/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/31 20:32
 * @Version: V1.0
 */
// @Data
// @Builder
public class OracleConnectorConfig extends OracleTaskConfig {
    /**
     * 读取模式
     * ALL 全量抽取
     * CURRENT 增量抽取
     * TIME 根据时间线抽取
     * SCN 根据指定的便宜量抽取
     */
    private DataReadType dataReadType;

    OracleConnectorConfig(String taskName, DataReadType readPosition, Long startTime, String endTime, String startSCN, String endSCN, int fetchSize, List<String> tables, int identification, String rs_id, JdbcConfig jdbcConfig, Short snapShotParallelism) {
        super(taskName, readPosition, startTime, endTime, startSCN, endSCN, fetchSize, tables, identification, rs_id, jdbcConfig, snapShotParallelism);
    }

    public DataReadType getDataReadType() {
        return dataReadType;
    }

    public void setDataReadType(DataReadType dataReadType) {
        this.dataReadType = dataReadType;
    }
}
