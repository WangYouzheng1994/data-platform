package org.raise.cdc.base.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description:
 * @Author: WangYouzheng
 * @Date: 2023/6/6 20:00
 * @Version: V1.0
 */
public class DateUtils {
    /**
     * 将字符串yyyy-MM-dd HH:mm:ss的时间转换为时间戳
     *
     * @param s
     * @return
     */
    public static String dateToStamp(String s) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }

    /**
     * parse time type data
     *
     * @param value
     * @return
     */
    public static String parseTime(String value) {
        if (!value.endsWith("')")) {
            return value;
        }
        //增加转换为时间戳
        // DATE类型
        if (value.startsWith("TO_DATE('")) {
            return  dateToStamp(value.substring(9, value.length() - 27));
        }
        ////增加转换为时间戳
        // TIMESTAMP类型
        if (value.startsWith("TO_TIMESTAMP('")) {
            return dateToStamp(value.substring(14, value.length() - 2));
        }
        ////增加转换为时间戳
        // TIMESTAMP类型
        if (value.startsWith("TIMESTAMP '")) {
            return dateToStamp(value.substring(12, value.length() - 1));
        }

        // TIMESTAMP WITH LOCAL TIME ZONE
        if (value.startsWith("TO_TIMESTAMP_ITZ('")) {
            return value.substring(18, value.length() - 2);
        }

        // TIMESTAMP WITH TIME ZONE 类型
        if (value.startsWith("TO_TIMESTAMP_TZ('")) {
            return value.substring(17, value.length() - 2);
        }
        return value;
    }
}
