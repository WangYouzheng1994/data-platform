package org.raise.cdc.oracle.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.raise.cdc.base.constants.ConstantValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Description: OracleCDC Mapper TODO: 转成单例的，为了后续能够直接操纵到Mapper层
 * @Author: WangYouzheng
 * @Date: 2023/5/31 20:30
 * @Version: V1.0
 */
@Slf4j
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
     * 获取V$LogContents的结果
     */
    public static final String SQL_SELECT_LOGCONTENTS =  ""
            + "SELECT\n"
            + "    scn,\n"
            + "    timestamp,\n"
            + "    operation,\n"
            + "    operation_code,\n"
            + "    seg_owner,\n"
            + "    table_name,\n"
            + "    sql_redo,\n"
            + "    sql_undo,\n"
            + "    xidusn,\n"
            + "    xidslt,\n"
            + "    xidsqn,\n"
            + "    row_id,\n"
            + "    rollback,\n"
            + "    csf,\n"
            +"     rs_id\n"
            + "FROM\n"
            + "    v$logmnr_contents\n";

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

    public static List<String> EXCLUDE_SCHEMAS = Collections.singletonList("SYS");

    /**
     * 构建查询v$logmnr_contents视图SQL
     *
     * @param listenerOptions 需要采集DML操作类型字符串 delete,insert,update，减少数据量
     * @param tableList 需要采集的schema+表名 SCHEMA1.TABLE1,SCHEMA2.TABLE2
     * @return
     */
    public static String selectLogContents(
            String listenerOptions, String tableList, boolean isCdb) {
        StringBuilder sqlBuilder = new StringBuilder(SqlUtil.SQL_SELECT_LOGCONTENTS);
        sqlBuilder.append(" where ");
        if (StringUtils.isNotEmpty(tableList)) {
            sqlBuilder.append("  ( ").append(buildSchemaTableFilter(tableList, isCdb));
        } else {
            sqlBuilder.append("  ( ").append(buildExcludeSchemaFilter());
        }
        //判断异常类型
        /*if(this.identification==4||this.identification==1){
            sqlBuilder.append(" and ").append(" rs_id > '"+this.rs_id+"'");
        }*/
        if (StringUtils.isNotEmpty(listenerOptions)) {
            sqlBuilder.append(" and ").append(buildOperationFilter(listenerOptions));
        }

        // 包含commit
        sqlBuilder.append(" or OPERATION_CODE = 7 )");
        String sql = sqlBuilder.toString();
        log.debug("SelectSql = {}", sql);
        return sql;
    }

    /**
     * 构建需要采集的schema+表名的过滤条件
     *
     * @param listenerTables 需要采集的schema+表名 SCHEMA1.TABLE1,SCHEMA2.TABLE2
     * @return
     */
    private static String buildSchemaTableFilter(String listenerTables, boolean isCdb) {
        List<String> filters = new ArrayList<>();

        String[] tableWithSchemas = listenerTables.split(ConstantValue.COMMA_SYMBOL);
        for (String tableWithSchema : tableWithSchemas) {
            List<String> tables = Arrays.asList(tableWithSchema.split("\\."));
            if (ConstantValue.STAR_SYMBOL.equals(tables.get(0))) {
                throw new IllegalArgumentException(
                        "Must specify the schema to be collected:" + tableWithSchema);
            }

            StringBuilder tableFilterBuilder = new StringBuilder(256);
            if (isCdb && tables.size() == 3) {
                tableFilterBuilder.append(String.format("SRC_CON_NAME='%s' and ", tables.get(0)));
            }

            tableFilterBuilder.append(
                    String.format(
                            "SEG_OWNER='%s'",
                            isCdb && tables.size() == 3 ? tables.get(1) : tables.get(0)));

            if (!ConstantValue.STAR_SYMBOL.equals(
                    isCdb && tables.size() == 3 ? tables.get(2) : tables.get(1))) {
                tableFilterBuilder
                        .append(" and ")
                        .append(
                                String.format(
                                        "TABLE_NAME='%s'",
                                        isCdb && tables.size() == 3
                                                ? tables.get(2)
                                                : tables.get(1)));
            }

            filters.add(String.format("(%s)", tableFilterBuilder));
        }

        return String.format("(%s)", StringUtils.join(filters, " or "));
    }

    /**
     * 过滤系统表
     *
     * @return
     */
    private static String buildExcludeSchemaFilter() {
        List<String> filters = new ArrayList<>();
        for (String excludeSchema : EXCLUDE_SCHEMAS) {
            filters.add(String.format("SEG_OWNER != '%s'", excludeSchema));
        }

        return String.format("(%s)", StringUtils.join(filters, " and "));
    }

    /**
     * 构建需要采集操作类型字符串的过滤条件
     *
     * @param listenerOptions 需要采集操作类型字符串 delete,insert,update
     * @return
     */
    private static String buildOperationFilter(String listenerOptions) {
        List<String> standardOperations = new ArrayList<>();

        String[] operations = listenerOptions.split(ConstantValue.COMMA_SYMBOL);
        for (String operation : operations) {

            int operationCode;
            switch (operation.toUpperCase()) {
                case "INSERT":
                    operationCode = 1;
                    break;
                case "DELETE":
                    operationCode = 2;
                    break;
                case "UPDATE":
                    operationCode = 3;
                    break;
                default:
                    throw new RuntimeException("Unsupported operation type:" + operation);
            }

            standardOperations.add(String.format("'%s'", operationCode));
        }

        return String.format(
                "OPERATION_CODE in (%s) ",
                StringUtils.join(standardOperations, ConstantValue.COMMA_SYMBOL));
    }
}
