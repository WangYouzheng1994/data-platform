package org.raise.cdc.base.data;

import java.sql.SQLException;

/**
 * @Description: 数据任务
 * @Author: WangYouzheng
 * @Date: 2023/5/30 14:31
 * @Version: V1.0
 */
public interface DataTask {
    /**
     * 对外提供启动方法
     */
    void execute();

    /**
     * 暂停
     */
    void pause();

    /**
     * 断点续传
     */
    default void contin() {

    }

    /**
     * 启动
     */
    default void start() {

    }

    /**
     * 重启（异常规避）
     */
    default void restart() {

    }

}
