package org.raise.cdc.oracle.mapper;

import org.apache.commons.lang3.StringUtils;

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

    /** 加载包含startSCN和endSCN之间日志的日志文件 */
    public static final String SQL_START_LOGMINER =
            "DECLARE \n"
                    + "    st          BOOLEAN := true;\n"
                    + "    start_scn   NUMBER := ?;\n"
                    + "    endScn   NUMBER := ?;\n"
                    + "BEGIN\n"
                    + "    FOR l_log_rec IN (\n"
                    + "        SELECT\n"
                    + "            MIN(name) name,\n"
                    + "            first_change#\n"
                    + "        FROM\n"
                    + "          (\n"
                    + "            SELECT \n"
                    + "              member AS name, \n"
                    + "              first_change# \n"
                    + "            FROM \n"
                    + "              v$log   l \n"
                    + "           INNER JOIN v$logfile   f ON l.group# = f.group# \n"
                    + "           WHERE (l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE' )\n"
                    + "           AND first_change# < endScn \n"
                    + "           UNION \n"
                    + "           SELECT  \n"
                    + "              name, \n"
                    + "              first_change# \n"
                    + "           FROM \n"
                    + "              v$archived_log \n"
                    + "           WHERE \n"
                    + "              name IS NOT NULL \n"
                    + "           AND STANDBY_DEST='NO'\n"
                    + "           AND  first_change# < endScn  AND next_change# > start_scn )group by first_change# order by first_change#  )LOOP IF st THEN \n"
                    + "  SYS.DBMS_LOGMNR.add_logfile(l_log_rec.name, SYS.DBMS_LOGMNR.new); \n"
                    + "      st := false; \n"
                    + "  ELSE \n"
                    + "  SYS.DBMS_LOGMNR.add_logfile(l_log_rec.name); \n"
                    + "  END IF; \n"
                    + "  END LOOP;\n"
                    + "  SYS.DBMS_LOGMNR.start_logmnr(       options =>          SYS.DBMS_LOGMNR.skip_corruption        + SYS.DBMS_LOGMNR.no_sql_delimiter        + SYS.DBMS_LOGMNR.no_rowid_in_stmt\n"
                    + "  + SYS.DBMS_LOGMNR.dict_from_online_catalog    );\n"
                    + "   end;";


    /**
     * 根据SCN闪回指定表的数据
     */
    public static final String SQL_FLASH_TABLE_SCN = "SELECT * FROM %s AS OF SCN %s";

    /**
     * 格式化SQL模板替换
     *
     * @param sqlTemplate
     * @param params
     * @return
     */
    public static String formatSQL(String sqlTemplate, String... params) {
        String result = StringUtils.EMPTY;
        if (StringUtils.isNotBlank(sqlTemplate)) {
            result = String.format(sqlTemplate, params);
        }
        return result;
    }
}
