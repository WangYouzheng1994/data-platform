package org.raise.cdc.base.monitor;


/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/31 13:33
 * @Version: V1.0
 */
public interface TaskMonitor {
    /**
     * 检测器初始化
     */
    void init();

    /**
     * 心跳检测
     * @return
     */
    boolean heartbeat();

    /**
     * 对外发出警告
     */
    void warning();

}
