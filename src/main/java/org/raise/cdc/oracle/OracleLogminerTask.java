package org.raise.cdc.oracle;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.raise.cdc.base.config.BaseContextConfig;
import org.raise.cdc.base.transaction.TransactionManager;
import org.raise.cdc.base.util.DataSourceUtil;
import org.raise.cdc.oracle.config.OracleTaskConfig;
import org.raise.cdc.oracle.connector.OracleConnector;
import org.raise.cdc.oracle.task.AbstractOracleTask;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static org.raise.cdc.base.util.PropertiesUtil.getPropsStr;

/**
 * @Description: Oracle抽数任务
 * @Author: WangYouzheng
 * @Date: 2023/5/30 14:31
 * @Version: V1.0
 */
@Slf4j
public class OracleLogminerTask extends AbstractOracleTask {


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
    private List<OracleConnector> connections;

    private ExecutorService executorService;

    /**
     * 控制任务的运转状态，此状态应该持久化DB，并且还需要面向zookeeper进行同步
     */
    private boolean runningFlag = true;

    // KafkaProducer<Object, Object> producer =

    /**
     * 加载配置
     */
    private void initReadConfig() {
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
     * 快照数据
     * 根据当前的currentScn，进行闪回数据生成，理论上来说如果snapshot速度快，数据少，这个阶段做完直接进redo，但是不排除数据多 或者日志调的太小，出完数进入到归档了
     */
    Boolean snapShot() throws SQLException {
        OracleConnector initialConnect = OracleConnector.init(this.contextConfig);
        // 当前的scn，理论上来说如果snapshot速度快，数据少，这个阶段做完直接进redo，但是不排除数据多 或者日志调的太小，出完数进入到归档了
        BigInteger currentScn = initialConnect.getCurrentScn();
        // initialConnect. TODO: 清除连接

        Short snapShotParallelism = oracleTaskConfig.getSnapShotParallelism();
        List<String> allTables = oracleTaskConfig.getTables();

        // 计算需要的分片
        List<List<String>> splitTables = Lists.partition(allTables, snapShotParallelism);

        if (CollectionUtils.isNotEmpty(splitTables)) {
            CompletableFuture[] snapShotResult = splitTables.stream().map(tables -> {
                OracleConnector split = null;
                try {
                    split = OracleConnector.init(this.contextConfig);
                    this.connections.add(initialConnect);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                return CompletableFuture.supplyAsync(FlashInitTableHandler.builder().connector(split).tables(tables).build(), executorService);
            }).toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(snapShotResult).join();
            log.info("snapshot 结束了哈~");
            return true;
        } else {
            log.error("没有指定要抽取的表，请设置tables[]");
            return false;
        }
    }

    /**
     * 初始化LogminerConnection
     */
    void startLogminerConnection() {

    }

    /**
     * 初始化任务
     */
    @Override
    public void init() throws SQLException, ClassNotFoundException {
        // 初始化配置
        this.initReadConfig();

        // 初始化jdbc连接池
        this.contextConfig.setDataSource(DataSourceUtil.getDataSource(this.oracleTaskConfig));

        // TODO： 使用snapshot + 增量抽取的并行度的和
        executorService = Executors.newFixedThreadPool(oracleTaskConfig.getSnapShotParallelism());

        // 计算要抽数的范围


        // 进入多线程抽取模式 猛抽~~
    }

    /**
     * 启动任务
     *
     * TODO: 此方法需要有一个控制器Wrapper来屏蔽掉。只允许开放executor
     */
    @Override
    public void start() {

        // 判定当前的模式，先默认全量转增量
        // 断点续传后面重新考虑实现，比如提供的scn已经失效，那么就还是全量转增量，暂时先实现scn的模式
        // TODO:这里要转成多线程ComplatebleFuture异步通知模式，自主启动全量转增量
        try {
            this.snapShot();
        } catch (SQLException e) {
            log.error("快照抽数异常！");
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 结束任务
     */
    @Override
    public void close() {

    }


    /**
     * 任务级别的创建OracleConnection，并且分配任务
     *
     * @return
     */
    private Boolean createSubTask() {
        return null;
    }

    /**
     * 闪回执行线程
     */
    @Builder
    @Data
    private class FlashInitTableHandler implements Supplier {
        private List<String> tables;
        private OracleConnector connector;

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
       /* @Override
        public Object call() throws Exception {

        }*/

        /**
         * Gets a result.
         *
         * @return a result
         */
        @Override
        public Object get() {
            if (CollectionUtils.isNotEmpty(tables)) {

            }
            return null;
        }
    }
}
