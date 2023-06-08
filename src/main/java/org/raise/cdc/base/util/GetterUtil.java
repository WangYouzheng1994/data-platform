package org.raise.cdc.base.util;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2022/6/23 16:16
 * @Version: V1.0
 */
public class GetterUtil {

    /**
     * 取得String
     *
     * @param obj
     * @return
     */
    public static String getString(Object obj) {
        if (isNull(obj)) {
            return "";
        }
        return String.valueOf(obj);
    }

    /**
     * 获取Int
     * @return
     */
    public static int getInt(Object obj) {
        if (isNull(obj)) {
            return 0;
        }
        if (obj instanceof Integer) {
            return (Integer) obj;
        }
        return Integer.parseInt(getString(obj));
    }

    /**
     * 获取字符串参数列表
     *
     * @param params
     * @return
     */
    public static String[] getStrPar(Object... params) {
        if (ArrayUtils.isNotEmpty(params)) {
            return Arrays.stream(params).map(GetterUtil::getString).toArray(String[]::new);
        } else {
            return new String[]{};
        }
    }

    /**
     * 对象判定空
     *
     * @param obj
     * @return
     */
    public static boolean isNull(Object obj) {
        return obj == null;
    }
}
