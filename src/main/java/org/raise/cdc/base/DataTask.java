package org.raise.cdc.base;

/**
 * @Description: 数据任务
 * @Author: WangYouzheng
 * @Date: 2023/5/30 14:31
 * @Version: V1.0
 */
public interface DataTask {

    /**
     * initialize
     */
    default void init() {

    }

    /**
     * 结束任务
     */
    default void close() {

    }

    /**
     * 暂停
     */
    default void pause(){

    }

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
