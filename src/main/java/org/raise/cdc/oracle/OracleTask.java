package org.raise.cdc.oracle;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.raise.cdc.base.config.BaseContextConfig;
import org.raise.cdc.base.data.DataTask;
import org.raise.cdc.base.transaction.TransactionManager;
import org.raise.cdc.base.util.DataSourceUtil;
import org.raise.cdc.oracle.config.OracleTaskConfig;

import java.sql.SQLException;
import java.util.List;

import static org.raise.cdc.base.util.PropertiesUtil.getPropsStr;

/**
 * @Description: Oracle抽数任务
 * @Author: WangYouzheng
 * @Date: 2023/5/30 14:31
 * @Version: V1.0
 */
@Slf4j
public class OracleTask implements DataTask {

    /**
     * 启动该任务的配置
     */
    private OracleTaskConfig oracleCDCConfig;

    /**
     * 缓存控制器，放在这一层 你可以认为任务后续会管控多线程模式下的抽取动作
     */
    private TransactionManager transactionManager;

    /**
     * 执行过程中的上下文配置
     */
    private BaseContextConfig contextConfig;

    /**
     * jdbc连接池
     */
    DruidDataSource dataSource;

    /**
     * 控制任务的运转状态，此状态应该迁移到DB
     */
    private boolean runningFlag = true;

    // KafkaProducer<Object, Object> producer =

    void initReadConfig() {
        /**
         * 说明1. startscn 》消费scn时  再起启动时候的入参为starscn
         *         2.startscn < 消费scn时  断点续传的scn为 消费scn+1 (其中会出现相同事务提交的数据，出现异常时候的bug)
         */
        String host = getPropsStr("cdc.oracle.hostname");
        String port = getPropsStr("cdc.oracle.port");
        String user = getPropsStr("cdc.oracle.username");
        String pwd = getPropsStr("cdc.oracle.password");
        String database = getPropsStr("cdc.oracle.database");
        String KafkaTopicName = getPropsStr("kafka.topic");
        String wechatKafkaTopicName = getPropsStr("wechat.kafka.topic");
        String listentKafkaTopicName = getPropsStr("listen.kafka.topic");
        String jdbcUrl = getPropsStr("cdc.oracle.jdbcurl");

        List<String> sourceTableList = null;
        try {
            sourceTableList = Lists.newArrayList();
            //sourceTableList = new ArrayList<>();
            //sourceTableList.addAll(Arrays.asList(tableArray));
        } catch (Exception e) {
            log.error("启动失败，请配置cdc要抽取检测的表");
            throw e;
        }
        // 任务配置
        this.oracleCDCConfig = OracleTaskConfig.builder().jdbcUrl(jdbcUrl).tables(sourceTableList).userName(user).password(pwd).build();
        // 本地LRU缓存
        this.transactionManager = new TransactionManager(1000L, 20);
        //
    }

    /**
     * 初始化datasource
     */
    void initJdbcDataSource() {

    }

    /**
     * 对外启动
     */
    @Override
    public void run() {
        try {
            init();
        } catch (Exception e) {
            log.error("初始化失败" + e.getMessage(), e);
            return;
        }
    }

    /**
     * 初始化资源
     */
    @Override
    public void init() throws SQLException, ClassNotFoundException {
        // 初始化配置
        initReadConfig();

        // 初始化jdbc连接池
        this.contextConfig.setDataSource(DataSourceUtil.getDataSource(this.oracleCDCConfig));

        // 初始化第一个线程的连接

        // 计算要抽数的范围

        // 计算需要的分片

        // 进入多线程抽取模式 猛抽~~
    }
}
