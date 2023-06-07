package org.raise.cdc.oracle.task;

import lombok.extern.slf4j.Slf4j;
import org.raise.cdc.base.data.DataTask;

import java.sql.SQLException;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/6/7 14:03
 * @Version: V1.0
 */
@Slf4j
public abstract class AbstractOracleTask implements DataTask {
    /**
     * 对外提供启动方法
     */
    @Override
    public void execute() {
        try {
            init();
            start();
        } catch (Exception e) {
            log.error("初始化失败" + e.getMessage(), e);
            return;
        }
    }

    /**
     * initialize
     */
    public abstract void init() throws SQLException, ClassNotFoundException;

    /**
     * 启动
     */
    public abstract void start();

    /**
     * 结束任务
     */
    public abstract void close();

    /**
     * 暂停
     */
    @Override
    public void pause() {
        close();
    }

    /**
     * 断点续传
     */
    @Override
    public void contin() {
        DataTask.super.contin();
    }

    /**
     * 重启（异常规避）
     */
    @Override
    public void restart() {
        close();
        try {
            init();
            start();
        } catch (Exception e) {
            log.error("初始化失败" + e.getMessage(), e);
            return;
        }
    }
}
