package org.raise.cdc.oracle;

import lombok.Data;

import java.math.BigInteger;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/5/30 16:16
 * @Version: V1.0
 */
@Data
public class LogContentRecord {
    /**
     * 偏移量
     */
    private BigInteger scn;
    /** undo语句* */
    private String sqlUndo;
    /** redo语句* */
    private String sqlRedo;
    /** 事务id撤销段号* */
    private String xidUsn;
    /** 事务id槽号* */
    private String xidSlt;
    /** 事务id序列号* */
    private String xidSqn;
    /** rowId* */
    private String rowId;

    /**
     * 表名
     */
    private String tableName;
    /** 是否发生了日志切割* */
    private boolean hasMultiSql;
    /** DML操作类型 1插入 2删除 3 更新* */
    private int operationCode;

    @Override
    public String toString() {
        return "RecordLog{"
                + "scn="
                + scn
                + ", sqlUndo='"
                + sqlUndo
                + '\''
                + ", sqlRedo='"
                + sqlRedo
                + '\''
                + ", xidusn='"
                + xidUsn
                + '\''
                + ", xidslt='"
                + xidSlt
                + '\''
                + ", xidSqn='"
                + xidSqn
                + '\''
                + ", rowId='"
                + rowId
                + '\''
                + ", tableName='"
                + tableName
                + '\''
                + ", hasMultiSql="
                + hasMultiSql
                + ", operationCode="
                + operationCode
                + '}';
    }
}
