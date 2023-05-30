package org.raise.cdc.base.data;

/**
 * @Description: 数据消费者
 * @Author: WangYouzheng
 * @Date: 2023/5/30 14:51
 * @Version: V1.0
 */
public interface DataProcess {
    /**
     * 对外输出数据
     * @return
     */
    int sink();

    /**
     * 处理数据，然后调用sink()推送
     *
     * @return
     */
    int process();

}
