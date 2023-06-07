package org.raise.cdc.oracle.config;

import lombok.Builder;
import lombok.Data;
import org.raise.cdc.base.config.DataReadType;


/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/31 20:32
 * @Version: V1.0
 */
@Data
@Builder
public class OracleConnectorConfig extends OracleTaskConfig {
    /**
     * 读取模式
     * ALL 全量抽取
     * CURRENT 增量抽取
     * TIME 根据时间线抽取
     * SCN 根据指定的便宜量抽取
     */
    private DataReadType dataReadType;
}
