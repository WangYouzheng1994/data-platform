package org.raise.cdc.base.util;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.Properties;

/**
 * 解析properties配置文件
 * 默认走的是dev环境。即：resources/cdc-dev.properties
 *
 * 多环境配置，请修改resources/application.properties
 */
@Slf4j
public class PropertiesUtil {
    private static Properties applicationProperties = null;

    /**
     * 当前打包环境 跟着maven的profile走的
     */
    public static String ACTIVE_TYPE = "dev";

    static {
        log.info("加载application.properties....");
        // 加载上下文环境版本: dev、prod
        try (InputStream resourceAsStream =
                     PropertiesUtil.class.getResourceAsStream("/application.properties")) {
            Properties pro = new Properties();
            pro.load(resourceAsStream);

            String val = pro.getProperty("spring.profiles.active");
            if (val != null) {
                ACTIVE_TYPE = val;
                log.info("当前application版本为【{}】", ACTIVE_TYPE);
            } else {
                log.warn("application.properties中没有读取到对应的配置信息，使用默认配置【{}】", ACTIVE_TYPE);
            }
        } catch (Exception e) {
            log.error("读取application.properties异常");
            log.error(e.getMessage(), e);
        }

        String configFilePath = "/cdc" + "-" + ACTIVE_TYPE + ".properties";

        try (InputStream resourceAsStream = PropertiesUtil.class.getResourceAsStream(configFilePath)) {
            Properties pro = new Properties();
            pro.load(resourceAsStream);
        } catch (Exception e) {
            log.error("加载配置文件【{}】异常", configFilePath);
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 获取当前环境版本下的配置文件。
     *
     * @return cn.hutool.setting.dialect.Props
     */
    public static Properties getProps() {
        // return new Properties("Oraclecdc-"+ACTIVE_TYPE+".properties");
        return applicationProperties;
    }

    /**
     * 获取字符串类型Value值
     *
     * @return java.lang.String
     */
    public static String getPropsStr(String key) {
        return GetterUtil.getString(getProps().getProperty(key));
    }

    /**
     * 获取 int类型Value值
     *
     * @return
     */
    public static int getPropsInt(String key) {
        return GetterUtil.getInt(getProps().getProperty(key));

    }

    /**
     * 获取当前环境版本下的 checkPoint
     *
     * @param checkpointSubDir 在公共目录下面的二级目录
     * @return java.lang.String
     */
    public static String getCheckpointStr(String checkpointSubDir) {
        return PropertiesUtil.getProps().getProperty("checkpoint.hdfs.url") + checkpointSubDir;
    }

    /**
     * 获取当前环境版本下的 checkPoint
     *
     * @param savePointSubDir 在公共目录下面的二级目录
     * @return java.lang.String
     */
    public static String getSavePointStr(String savePointSubDir) {
        return PropertiesUtil.getProps().getProperty("savepoint.hdfs.url") + savePointSubDir;
    }
}
