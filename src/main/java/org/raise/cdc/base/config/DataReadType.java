package org.raise.cdc.base.config;

/**
 * @Description: 摄取类型Type
 * @Author: WangYouzheng
 * @Date: 2022/8/3 16:17
 * @Version: V1.0
 */
public enum DataReadType {
    ALL,// 全量抽取
    CURRENT,// 增量抽取
    TIME, // 根据时间线抽取
    SCN // 根据指定的便宜量抽取
}
