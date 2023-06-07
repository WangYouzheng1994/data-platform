package org.raise.cdc.base.data;

/**
 * @Description: TODO 加入抽象方法，默认对外只开放一个方法，然后要先调用process()
 * @Author: WangYouzheng
 * @Date: 2023/5/30 19:18
 * @Version: V1.0
 */
public class DataProcessKafka implements DataProcess {

    /**
     * 对外输出数据
     *
     * @return
     * @param obj
     */
    @Override
    public int sink(Object obj) {
        // 新增自定义处理器
        process(obj);
        return 0;
    }

    /**
     * 处理数据，然后调用sink()推送
     *
     * @return
     */
    public Object process(Object obj) {
        return obj;
    }
}
