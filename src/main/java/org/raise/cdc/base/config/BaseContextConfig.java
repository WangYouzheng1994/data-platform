package org.raise.cdc.base.config;

import lombok.Data;
import org.raise.cdc.oracle.config.OracleTaskConfig;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Description: 运行环境下的任务级上下文配置，用以容错的二次加载，以及预留任务在web数据中台的可视化的动态调整能力
 * @Author: WangYouzheng
 * @Date: 2023/5/31 13:31
 * @Version: V1.0
 */
@Data
public class BaseContextConfig {
    /**
     * 当前任务上下文的数据源连接池
     */
    private DataSource dataSource;

    // 指定SCN抽取  SCN模式
    private AtomicLong startSCN;

    // 指定SCN结束  SCN模式
    private AtomicLong endSCN;

    /**
     * 当前执行的SCN
     */
    private AtomicLong currentSCN;

    /**
     * 启动任务的参数数据
     */
    private OracleTaskConfig taskConfig;

    private TaskStatus taskStatus;

    /**
     * 任务状态 初始化组件中，快照抽取中（全量），日志抽取中(binlog/archievelog)，实时抽驱中(最新的redo/binlog)
     */
    enum TaskStatus {

    }
}
