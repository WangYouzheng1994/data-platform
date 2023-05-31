package org.raise.cdc.oracle;

import com.google.common.collect.Lists;
import org.raise.cdc.base.data.DataTask;
import org.raise.cdc.base.transaction.TransactionManager;
import org.raise.cdc.base.util.PropertiesUtil;
import org.raise.cdc.oracle.config.OracleCDCConfig;

import java.util.List;

import org.raise.cdc.base.util.PropertiesUtil;

import static org.raise.cdc.base.util.PropertiesUtil.getPropsStr;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/30 14:31
 * @Version: V1.0
 */
public class OracleTask implements DataTask {

    /**
     * 启动该任务的配置
     */
    private OracleCDCConfig oracleCDCConfig;

    /**
     * 缓存控制器，放在这一层 你可以认为任务后续会管控多线程模式下的抽取动作
     */
    private TransactionManager transactionManager;

    /**
     * 控制任务的运转状态，此状态应该迁移到DB
     */
    private boolean runningFlag = true;

    // KafkaProducer<Object, Object> producer =

    void readConfig() {
        /***
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
        }catch (Exception e){
            e.printStackTrace();
        }
        this.oracleCDCConfig = OracleCDCConfig.builder().jdbcUrl(jdbcUrl).tables(sourceTableList).username(user).password(pwd).build();


        /*String host = PropertiesUtil.getPropsStr()("cdc.oracle.hostname");
        String port = props.getStr("cdc.oracle.port");
        String tableArray = props.getStr("cdc.oracle.table.list");
        String user = props.getStr("cdc.oracle.username");
        String pwd = props.getStr("cdc.oracle.password");
        String oracleServer = props.getStr("cdc.oracle.database");
        KafkaTopicName = props.getStr("kafka.topic");
        wechatKafkaTopicName = props.getStr("wechat.kafka.topic");
        listentKafkaTopicName = props.getStr("listen.kafka.topic");
        schema = props.getStr("cdc.oracle.schema.list");
        String scnstr = props.getStr("cdc.scnscope");
        String scnstrmin = props.getStr("cdc.scnscopemin");
        List<String> sourceTableList = null;
        startTime = props.getStr("starttime");
        endTime = props.getStr("endtime");
        startTimeAfter = props.getStr("startimeafter");
        endTimeAfter = props.getStr("endtimeafter");
        scnScope = Integer.valueOf(scnstr);
        scnScopeMin = Integer.valueOf(scnstrmin);
        lastScnScope = scnScope;
        try {
            sourceTableList = getSourceTableList();
            //sourceTableList = new ArrayList<>();
            //sourceTableList.addAll(Arrays.asList(tableArray));
        }catch (Exception e){
            e.printStackTrace();
        }*/
    }

    /**
     * 初始化资源
     */
    @Override
    public void init() {
        // 初始化配置
        readConfig();



    }
}
