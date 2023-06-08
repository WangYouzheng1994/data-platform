package org.raise.cdc.base.config;

import lombok.Data;
import org.raise.cdc.base.transaction.TransactionManager;
import org.raise.cdc.oracle.config.OracleTaskConfig;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * 缓存控制器，放在这一层 你可以认为任务后续会管控多线程模式下的抽取动作
     */
    private TransactionManager transactionManager;

    /**
     * 指定SCN抽取  SCN模式
     */
    private AtomicLong startSCN;

    /**
     * 指定SCN结束  SCN模式
     */
    private AtomicLong endSCN;

    /**
     * 每次SCN的步进量
     */
    private AtomicInteger stepSCN;

    /**
     * 当前执行的SCN
     */
    private AtomicLong currentSCN;

    /**
     * 实际抽取到的数据的SCN号，因此当前的这个版本jdbc的抽取fetch和stepSCN是相同参数，切记步进过大会内存溢出，多线程就是时间换空间，此版本可以优化成 先count 然后让多个线程拉齐，（以后再说吧）
     */
    private AtomicLong currentPositionSCN;


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

    /**
     * 获取当前任务的抽取起始，作为下一轮的START
     * 算法解释： 用当前end，
     * TODO:如果是redo那么就再+1 考虑要放到各个执行线程去做，因为有的线程可能还在挖掘归档，有的就到redo了
     * curretnSCN + (stepSCN/并发数)
     *
     * @return
     */
    public Long getCurrentStartSCN() {
        long nowStart = endSCN.get();
        startSCN.set(nowStart);
        return nowStart;
    }

    /**
     * 获取当前任务抽取的结束
     * 用计算后的start + step
     * @return
     */
    public Long getCurrentEndSCN() {
        endSCN.set(startSCN.addAndGet(stepSCN.get()));
        return endSCN.get();
    }
}
