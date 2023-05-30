package org.raise.cdc.oracle;

import org.raise.cdc.base.data.DataTask;
import org.raise.cdc.base.transaction.TransactionManager;
import org.raise.cdc.oracle.config.OracleCDCConfig;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/30 14:31
 * @Version: V1.0
 */
public class OracleTask implements DataTask {

    /**
     * 启动该任务的配置
     */
    private OracleCDCConfig oracleCDCConfig;

    /**
     * 缓存控制器，放在这一层 你可以认为任务后续会管控多线程模式下的抽取动作
     */
    private TransactionManager transactionManager;

    /**
     * 控制任务的运转状态，此状态应该迁移到DB
     */
    private boolean runningFlag = true;

    // KafkaProducer<Object, Object> producer =
}
