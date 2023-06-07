package org.raise.cdc.oracle.connector;

import com.alibaba.fastjson2.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.raise.cdc.base.config.BaseContextConfig;
import org.raise.cdc.base.config.DataReadType;
import org.raise.cdc.base.data.DataProcess;
import org.raise.cdc.base.util.JDBCConnector;
import org.raise.cdc.oracle.config.OracleConnectorConfig;
import org.raise.cdc.oracle.constants.LogminerKeyConstants;
import org.raise.cdc.oracle.mapper.SqlUtil;

import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.raise.cdc.base.util.DateUtils.dateToStamp;

/**
 * @Description: 核心处理类，线程（JDBCConnect也是一个链接）
 * @Author: WangYouzheng
 * @Date: 2023/5/31 15:24
 * @Version: V1.0
 */
@Data
@Slf4j
public class OracleConnector extends JDBCConnector {
    private OracleConnectorContext connContext;
    // private Connection connection;

    /**
     * OracleConnector静态工厂
     *
     * @param contextConfig
     */
    public static OracleConnector init(BaseContextConfig contextConfig) throws SQLException {
        OracleConnector oracleConnector = new OracleConnector();
        oracleConnector.internalInit(contextConfig);
        return oracleConnector;
    }

    /**
     * 内部初始化
     *
     * @param contextConfig
     * @throws SQLException
     */
    void internalInit(BaseContextConfig contextConfig) throws SQLException {
        this.connContext = new OracleConnectorContext<>();
        connContext.setContextConfig(contextConfig);
        this.connection = contextConfig.getDataSource().getConnection();
    }

    /**
     * 获取起始SCN号
     *
     * @param startScn
     * @return
     */
    public BigInteger getStartSCN(BigInteger startScn) {
        // 如果从保存点模式开始 并且不是0 证明保存点是ok的
        if (startScn != null && startScn.compareTo(BigInteger.ZERO) != 0) {
            return startScn;
        }
        OracleConnectorConfig connectorConfig = connContext.getConnectorConfig();
        DataReadType dataReadType = connectorConfig.getDataReadType();

        // 恢复位置为0，则根据配置项进行处理
        if (DataReadType.ALL.equals(dataReadType)) {
            // 获取最开始的scn
            startScn = getMinScn();
        } else if (DataReadType.CURRENT.equals(dataReadType)) {
            startScn = getCurrentScn();
        } else if (DataReadType.TIME.name().equals(dataReadType)) {
            // 根据指定的时间获取对应时间段的日志文件的起始位置

        }/* else if (DataReadType.SCN.name().equalsIgnoreCase(oracleCDCConfig.getReadPosition())) {
            // 根据指定的scn获取对应日志文件的起始位置

        } */ else {

        }
        return startScn;
    }

    /**
     * 获取最小SCN
     *
     * @return
     */
    public BigInteger getMinScn() {
        BigInteger minScn = null;
        PreparedStatement minScnStmt = null;
        ResultSet minScnResultSet = null;

        try {
            minScnStmt = connection.prepareStatement(SqlUtil.SQL_GET_LOG_FILE_START_POSITION);

            minScnResultSet = minScnStmt.executeQuery();
            while (minScnResultSet.next()) {
                minScn = new BigInteger(minScnResultSet.getString(LogminerKeyConstants.KEY_FIRST_CHANGE));
            }

            return minScn;
        } catch (SQLException e) {
            log.error(" obtaining the starting position of the earliest archive log error", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(minScnResultSet, minScnStmt, null);
        }
    }

    /**
     * 获取当前SCN （即最大SCN号） 适用于增量模式
     *
     * @return
     */
    public BigInteger getCurrentScn() {
        BigInteger currentScn = null;
        PreparedStatement currentScnStmt = null;
        ResultSet currentScnResultSet = null;

        try {
            currentScnStmt = connection.prepareStatement(SqlUtil.SQL_GET_CURRENT_SCN);

            currentScnResultSet = currentScnStmt.executeQuery();
            while (currentScnResultSet.next()) {
                currentScn = new BigInteger(currentScnResultSet.getString(LogminerKeyConstants.KEY_CURRENT_SCN));
            }
            return currentScn;
        } catch (SQLException e) {
            log.error("获取当前的SCN出错:", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(currentScnResultSet, currentScnStmt, null);
        }
    }


    // --------------- 快照阶段 ------

    /**
     * 初始化全量数据
     * @deprecated TODO:此数据逻辑涉及到scn的 应该提取到 task层，由task下发给connector执行
     */
    @Deprecated
    private void initOracleAllData() {
        OracleConnectorConfig connectorConfig = connContext.getConnectorConfig();
        //获取当前需要查询的数据表
        List<String> tableList = connectorConfig.getTables();
        // 获取当前数据库的最大偏移量
        BigInteger currentMaxScn = getCurrentScn();
        //遍历
/*        for (String tableName : tableList) {
            log.info("初始化表：" + tableName);
            doInitOracleAllData(currentMaxScn, tableName, );
        }*/
        //赋值startscn 与endscn
/*        this.startScn =currentMaxScn;
        this.endScn =currentMaxScn;
        oracleCDCConnect.setEndScn(this.endScn);
        this.currentSinkPosition =this.endScn;*/
        //输出一下日志
        log.info("初始化init 闪回数据已完毕");
    }

    /**
     * 根据scn，获取指定表的全量闪回数据
     *  @param currentMaxScn
     * @param tableName
     * @param sinkProcess
     */
    public void doInitOracleAllData(BigInteger currentMaxScn, String tableName, DataProcess sinkProcess) {
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String sql = SqlUtil.formatSQL(SqlUtil.SQL_FLASH_TABLE_SCN, tableName, currentMaxScn.toString());
            preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(10000);
            preparedStatement.setQueryTimeout(600);
            //增加参数
            //preparedStatement.setString(1,tableName);
            //preparedStatement.setInt(2,currentMaxScn.intValue());
            //执行
            //log.info("表："+tableName+",开始查询");
            rs = preparedStatement.executeQuery();
            //log.info("表："+tableName+",查询结束");
            //检索列名列表
            ResultSetMetaData rsMetaData = rs.getMetaData();
            int count = rsMetaData.getColumnCount();
            //封装数据，如果是date类型额外处理
            List<String> columnList = new ArrayList<>();
            List<String> typeList = new ArrayList<>();
            for (int i = 1; i <= count; i++) {
                typeList.add(rsMetaData.getColumnTypeName(i));
                columnList.add(rsMetaData.getColumnName(i));
            }
            //取数据
            //int j=0;
            Map<String, String> map = null;
            Map<String, Object> kafkaData = new HashMap<>();
            while (rs.next()) {
                // startTime=System.currentTimeMillis();
                map = new HashMap<>();
                //遍历拼接数据
                for (int i = 0; i < columnList.size(); i++) {
                    String colum = columnList.get(i);
                    String value = rs.getString(colum);
                    //判断如果是日期类型
                    if ("DATE".equals(typeList.get(i))) {
                        //格式化时间
                        if (StringUtils.isNoneBlank(value))
                            value = dateToStamp(value);
                    }
                    if ("TIMESTAMP".equals(typeList.get(i))) {
                        //格式化时间
                        if (StringUtils.isNoneBlank(value))
                            value = dateToStamp(value);
                    }
                    map.put(colum, value);
                }
                // TODO：此处需要做成动态的序列化接口，借以适配下游的自定义能力，因此应该把数据推送给本身组件内部的缓存。

                //封装kafkaDate数据
                kafkaData.put("after", map);
                kafkaData.put("before", "");
                // kafkaData.put("database",schema);
                kafkaData.put("scn", currentMaxScn);
                kafkaData.put("tableName", tableName.split("\\.")[1]);
                kafkaData.put("ts", System.currentTimeMillis());
                kafkaData.put("type", "insert");
                //json格式输出数据
                String kafkaStr = JSON.toJSONString(kafkaData);
                // 推送给内部缓存
                sinkProcess.sink(kafkaStr);
                // producer.send(new ProducerRecord<>(KafkaTopicName, kafkaStr));
            }
        } catch (SQLException e) {
            log.info("初始化数据ing失败");
            e.printStackTrace();
            /*TextMsgBean textMsgBean=new TextMsgBean();
            textMsgBean.setMsgtype("text");
            TextMsgFieldBean textMsgFieldBean = new TextMsgFieldBean();
            textMsgFieldBean.setContent("初始化全量数据失败~"+e.getMessage());
            textMsgBean.setText(textMsgFieldBean);
            producer.send(new ProducerRecord<>(wechatKafkaTopicName, JSONObject.toJSONString(textMsgBean)));*/
            //初始化失败
            throw new RuntimeException(e);
        } finally {
            try {
                preparedStatement.close();
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
