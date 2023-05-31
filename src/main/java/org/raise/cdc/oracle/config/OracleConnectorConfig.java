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
    private DataReadType dataReadType;
}
