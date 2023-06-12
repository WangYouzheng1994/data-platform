package org.raise.cdc.oracle;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/6/12 16:06
 * @Version: V1.0
 */

import lombok.Builder;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.raise.cdc.base.data.DataProcessKafka;
import org.raise.cdc.oracle.connector.OracleConnector;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Supplier;

/**
 * 闪回执行线程
 */
@Builder
@Data
public class FlashInitTableHandler implements Supplier<Boolean>  {
    /**
     * 要抽取的表
     */
    private List<String> tables;
    /**
     * 连接任务
     */
    private OracleConnector connector;
    /**
     * 最大的scn水位线
     */
    private BigInteger currentMaxScn;

    /**
     * Gets a result.
     *
     * @return a result
     */
    @Override
    public Boolean get() {
        if (CollectionUtils.isNotEmpty(tables) && connector != null) {
            tables.stream().forEach(tableName -> {
                connector.doInitOracleAllData(currentMaxScn, tableName, new DataProcessKafka());
            });
            return true;
        }
        return false;
    }
}
