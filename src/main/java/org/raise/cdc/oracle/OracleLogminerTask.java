package org.raise.cdc.oracle;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.raise.cdc.base.config.BaseContextConfig;
import org.raise.cdc.base.data.DataTask;
import org.raise.cdc.base.transaction.TransactionManager;
import org.raise.cdc.base.util.DataSourceUtil;
import org.raise.cdc.oracle.config.OracleTaskConfig;
import org.raise.cdc.oracle.connector.OracleConnector;

import java.sql.Connection;
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
public class OracleLogminerTask implements DataTask {

    /**
     * 启动该任务的配置
     */
    private OracleTaskConfig oracleTaskConfig;

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
    private DruidDataSource dataSource;

    /**
     * 当前任务所有的jdbc链接（其实就是每个logminer）
     * TODO： 后续要转成OracleConnection
     */
    private List<Connection> connections;

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
        this.oracleTaskConfig = OracleTaskConfig.builder().jdbcUrl(jdbcUrl).tables(sourceTableList).userName(user).password(pwd).build();
        this.contextConfig = new BaseContextConfig();
        this.contextConfig.setTaskConfig(oracleTaskConfig);
        // 本地LRU缓存
        this.transactionManager = new TransactionManager(1000L, 20);
        //
    }

    /**
     * 从数据源中获取jdbc链接
     */
/*    void getConnection() throws SQLException {
        *//*Connection connection = this.contextConfig.getDataSource().getConnection();
        try (PreparedStatement preparedStatement =
                     connection.prepareStatement(SqlUtil.SQL_ALTER_NLS_SESSION_PARAMETERS)) {
            // preparedStatement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
            preparedStatement.execute();
            interval = 5;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            closeResources(null, null, connection);
            interval++;
        }*//*
        this.connections.add(this.contextConfig.getDataSource().getConnection());
    }*/

    void snapShot() {

    }

    /**
     * 初始化LogminerConnection
     */
    void startLogminerConnection() {

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
        this.initReadConfig();

        // 初始化jdbc连接池
        this.contextConfig.setDataSource(DataSourceUtil.getDataSource(this.oracleTaskConfig));
        OracleConnector initialConnect = OracleConnector.init(this.contextConfig);

        // 判定当前的模式，先默认全量转增量
        // 断点续传后面重新考虑实现，比如提供的scn已经失效，那么就还是全量转增量，暂时先实现scn的模式
        // TODO:这里要转成多线程ComplatebleFuture异步通知模式，自主启动全量转增量
        this.snapShot();


        // 初始化第一个jdbc连接
        // getConnection();
        //

        // 计算要抽数的范围

        // 计算需要的分片

        // 进入多线程抽取模式 猛抽~~
    }
}
