package org.raise.cdc.oracle.mapper;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/31 20:30
 * @Version: V1.0
 */
public class SqlUtil {

    // 修改当前会话的date日期格式
    public static final String SQL_ALTER_NLS_SESSION_PARAMETERS =
            "ALTER SESSION SET "
                    + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                    + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'"
                    + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'";

    public static final String SQL_GET_LOG_FILE_START_POSITION = "";

    public static final String SQL_GET_CURRENT_SCN = "select min(CURRENT_SCN) CURRENT_SCN from gv$database";
}
