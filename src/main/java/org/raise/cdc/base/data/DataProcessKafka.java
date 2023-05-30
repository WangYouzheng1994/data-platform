package org.raise.cdc.base.data;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/30 19:18
 * @Version: V1.0
 */
public class DataProcessKafka implements DataProcess {

    /**
     * 对外输出数据
     *
     * @return
     */
    @Override
    public int sink() {
        return 0;
    }

    /**
     * 处理数据，然后调用sink()推送
     *
     * @return
     */
    @Override
    public int process() {
        return 0;
    }
}
