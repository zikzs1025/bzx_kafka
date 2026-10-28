package com.example.kafkaloader.file;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据表字段清单（唯一事实来源）：字段顺序 = 生成文件每行的列顺序。
 * <p>
 * 与《数据表字段清单.xlsx》严格一一对应，共 31 列，顺序不可随意调整——
 * 下游按位置装载，顺序错了不会报错，只会静默串列。
 * <p>
 * 修改此文件后必须跑 {@link RecordRendererTest} 校验列数，并同步下游装载配置。
 */
public final class DataSchema {

    /** 字段类型，仅用于文档与建表脚本对照；文件里都是文本，不做类型转换 */
    public enum Type {
        STRING, BIGINT, DATETIME
    }

    /** 一个字段：名称 + 类型 + 中文描述 */
    public static final class Field {
        private final String name;
        private final Type type;
        private final String description;

        Field(String name, Type type, String description) {
            this.name = name;
            this.type = type;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public Type getType() {
            return type;
        }

        public String getDescription() {
            return description;
        }
    }

    /** 31 个字段，顺序即列顺序（对应清单序号 1~31） */
    public static final List<Field> FIELDS;

    /** 字段名 -> 列下标，用于 JSON 取值时定位 */
    private static final Map<String, Integer> INDEX;

    static {
        List<Field> fields = new ArrayList<>(31);
        fields.add(new Field("md_id", Type.STRING, "表主键"));
        fields.add(new Field("clue_id", Type.STRING, "线索ID/日志查询标识"));
        fields.add(new Field("clue_src_sys", Type.STRING, "线索布控/日志查询源"));
        fields.add(new Field("clue_dst_sys", Type.STRING, "线索布控目的地址/日志产生源"));
        fields.add(new Field("capture_time", Type.BIGINT, "截获时间(上线时间)"));
        fields.add(new Field("protocol", Type.STRING, "协议代码(本数据)"));
        fields.add(new Field("original_protocol", Type.STRING, "原始数据协议"));
        fields.add(new Field("data_source3", Type.STRING, "数据来源系统分类代码"));
        fields.add(new Field("mwbs", Type.STRING, "记录唯一标识"));
        fields.add(new Field("sensitive_level", Type.STRING, "数据敏感级别"));
        fields.add(new Field("saved_time", Type.BIGINT, "信息入库时间"));
        fields.add(new Field("remark", Type.STRING, "数据来源系统备注"));
        fields.add(new Field("data_check_id", Type.STRING, "对账标识ID"));
        fields.add(new Field("bunit", Type.STRING, "采集地单位名称"));
        fields.add(new Field("data_set_name", Type.STRING, "科信数据集名称"));
        fields.add(new Field("dzwjwz", Type.STRING, "电子文件位置"));
        fields.add(new Field("asjxrybh", Type.STRING, "案事件相关人员编号"));
        fields.add(new Field("asjbh", Type.STRING, "案事件编号"));
        fields.add(new Field("zjhm", Type.STRING, "常用证件 证件号码"));
        fields.add(new Field("xm", Type.STRING, "姓名"));
        fields.add(new Field("xbdm", Type.STRING, "性别"));
        fields.add(new Field("xbmc", Type.STRING, "性别名称"));
        fields.add(new Field("mzdm", Type.STRING, "民族"));
        fields.add(new Field("mzmc", Type.STRING, "民族名称"));
        fields.add(new Field("csrq", Type.DATETIME, "出生日期 日期值下限"));
        fields.add(new Field("xzzqhdm", Type.STRING, "户籍地址 行政区划代码"));
        fields.add(new Field("xzzqhmc", Type.STRING, "户籍地址 地址名称"));
        fields.add(new Field("criminal_time", Type.DATETIME, "抓获日期"));
        fields.add(new Field("gajgjgdm", Type.STRING, "抓获单位 公安机关机构代码"));
        fields.add(new Field("gajgjmc", Type.STRING, "抓获单位 公安机关名称"));
        fields.add(new Field("synchro_time", Type.DATETIME, "更新时间"));
        FIELDS = Collections.unmodifiableList(fields);

        Map<String, Integer> index = new HashMap<>(fields.size() * 2);
        for (int i = 0; i < fields.size(); i++) {
            index.put(fields.get(i).getName(), i);
        }
        INDEX = Collections.unmodifiableMap(index);
    }

    /** 列数，固定 31 */
    public static final int SIZE = FIELDS.size();

    private DataSchema() {
    }

    /** 字段名对应的列下标，不存在返回 {@code null}（payload 里常见多余字段，直接忽略） */
    static Integer indexOf(String name) {
        return INDEX.get(name);
    }

    public static List<Field> fields() {
        return FIELDS;
    }
}
