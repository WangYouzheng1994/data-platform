package org.raise.cdc.base.config;

import lombok.Data;

import javax.sql.DataSource;

/**
 * @Description: 运行环境下的任务级上下文配置，用以容错的二次加载，以及任务可视化中的动态调整
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
}
