package org.raise.cdc.oracle.config;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @Description: 开启一个CDC 任务的参数，此类需要考虑持久化到数据库，从数据库加载。
 * @Author: WangYouzheng
 * @Date: 2022/8/3 13:19
 * @Version: V1.0
 */
@Data
@Builder
public class OracleCDCConfig {
    // 默认是增量抽取：除此以外的模式为：all， time, scn
    private String readPosition = "all";

    // 指定时间抽取  time模式
    private Long startTime;

    // 指定时间抽取  time模式
    private String endTime;

    // 指定SCN抽取  SCN模式
    private String startSCN;

    // 指定SCN结束  SCN模式
    private String endSCN;

    // 查询logminer 解析结果 v$logmnr_contents的数量
    private int fetchSize = 1000;

    // 要过滤的表 schema.TableName 大写
    private List<String> table;

    //处理日志为相同SCN数据得情况，以及异常出错位置[1.正常记录 2.创建日志报错 3.获取试图报错  4.写入数据逻辑报错]
    private int identification;

    //V$LOGMNR_CONTENTS 的唯一值
    private String rs_id;

    /**
     * jdbc 驱动连接
     */
    private String jdbcUrl;

    /**
     * jdbc username
     */
    private String username;

    /**
     * jdbc password
     */
    private String password;

    /**
     * jdbc 驱动类class
     */
    private String driverClass;

}
