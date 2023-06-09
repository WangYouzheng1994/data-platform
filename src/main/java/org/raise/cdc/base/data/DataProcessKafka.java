package org.raise.cdc.base.data;

import lombok.Data;
import lombok.Setter;

import java.util.List;

/**
 * @Description: TODO 加入抽象方法，默认对外只开放一个方法，然后要先调用process()
 * @Author: WangYouzheng
 * @Date: 2023/5/30 19:18
 * @Version: V1.0
 */
@Setter
public class DataProcessKafka implements DataProcess {
    private List arrayList;

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
     * TODO：因为当前版本涉及到多线程有序推数据的问题，需要考虑是否要在此方法中进行二次的排序。还在构思中。。。
     *
     * @return
     */
    public Object process(Object obj) {
        return obj;
    }
}
