package com.example.kafkaloader.file;

import com.example.kafkaloader.config.FilePipelineProperties;
import com.example.kafkaloader.model.KafkaMessageRecord;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 把 Kafka 消息体渲染成《数据表字段清单》定义的 31 列，字段之间用 {@code \t} 分隔。
 * <p>
 * 一条消息可能包含**多行**业务数据（现场上游按「批」投递，一个消息里几百上千行），
 * 因此按行拆分后逐行解析，一行产出一行结果。
 * <p>
 * 支持两种行内形态（由 {@code file.parse-mode} 决定）：
 * <ol>
 *   <li><b>JSON 对象</b>：按字段名精确取值，与键顺序无关</li>
 *   <li><b>分隔符文本</b>：按 {@code file.delimiter-regex}（默认制表符）切分，按位置装载；
 *       空字段靠连续分隔符保留位置，因此不能用 {@code \s+} 这类合并式正则</li>
 * </ol>
 * 取值规则：
 * <ul>
 *   <li>值原样输出，不做类型转换（BIGINT / DATETIME 按文本直落），避免精度与时区丢失</li>
 *   <li>缺失值写 {@code \N}（与空字符串区分，下游可识别为 NULL）</li>
 *   <li>制表符 / 回车 / 换行替换为空格，否则整行列错位</li>
 * </ul>
 * 任一行无法映射则整条消息进入隔离通道（不写半截数据），并给出具体行号与原因。
 */
class RecordRenderer {

    private static final Logger log = LoggerFactory.getLogger(RecordRenderer.class);

    /** 解析模式 */
    enum Mode {
        /** 以 { 开头按 JSON，否则按分隔符 */
        AUTO,
        /** 只按 JSON 对象解析 */
        JSON,
        /** 只按分隔符切分 */
        DELIMITER,
        /** 原样透传：不做任何解析与校验，报文行内 TAB 保持原样输出（空字段即空字符串） */
        RAW
    }

    /** 输出字段分隔符：制表符 */
    private static final char DELIMITER = '\t';

    /** 空值占位符常量：仅隔离文件里的时间戳等字段使用；主数据文件的空字段一律输出空字符串 */
    static final String NULL_VALUE = "\\N";

    private final JsonFactory jsonFactory = new JsonFactory();
    private final Mode mode;
    private final Pattern separator;
    private final boolean strictFieldCount;
    /** 期望列数；0 表示自适应（按报文实际列数输出） */
    private final int expectedColumns;
    /** 列映射：第 i 个源列落到清单的第 columnMapping[i] 列（1 基）；null 表示按位置直接对应 */
    private final int[] columnMapping;

    /** 同类告警只打一次，避免每批刷屏 */
    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    /** 默认：AUTO + 制表符分隔 + 严格 31 列 */
    RecordRenderer() {
        this(Mode.AUTO, "\t", true, DataSchema.SIZE, null);
    }

    RecordRenderer(FilePipelineProperties properties) {
        this(parseMode(properties.getParseMode()),
                properties.getDelimiterRegex() == null ? "\t" : properties.getDelimiterRegex(),
                properties.isStrictFieldCount(),
                properties.getExpectedColumns(),
                parseMapping(properties.getColumnMapping(), DataSchema.SIZE));
    }

    RecordRenderer(Mode mode, String delimiterRegex, boolean strictFieldCount) {
        this(mode, delimiterRegex, strictFieldCount, DataSchema.SIZE, null);
    }

    RecordRenderer(Mode mode, String delimiterRegex, boolean strictFieldCount, int[] columnMapping) {
        this(mode, delimiterRegex, strictFieldCount, DataSchema.SIZE, columnMapping);
    }

    RecordRenderer(Mode mode, String delimiterRegex, boolean strictFieldCount,
                   int expectedColumns, int[] columnMapping) {
        this.mode = mode;
        this.separator = Pattern.compile(delimiterRegex);
        this.strictFieldCount = strictFieldCount;
        this.expectedColumns = expectedColumns;
        this.columnMapping = columnMapping;
    }

    /**
     * 渲染消息体。返回 {@code null} 表示无法映射，调用方需走隔离通道（兼容旧调用方）。
     */
    String csvRow(KafkaMessageRecord record) {
        Result result = parse(record);
        return result.isOk() && !result.getRows().isEmpty() ? result.getRows().get(0) : null;
    }

    /**
     * 渲染消息体：一条消息可产出多行（多行报文），失败时给出原因（带行号）供隔离文件记录。
     */
    Result parse(KafkaMessageRecord record) {
        String payload = record.getPayload();
        if (payload == null || payload.trim().isEmpty()) {
            return Result.fail("empty-payload");
        }
        List<String> rows = new ArrayList<>(4);
        List<BadLine> badLines = new ArrayList<>();
        String adjustReason = null;
        String[] lines = payload.split("\r?\n", -1);
        for (int i = 0; i < lines.length; i++) {
            String line = stripLineEnd(lines[i]);
            if (line.trim().isEmpty()) {
                // 容忍报文尾部/中间的空行
                continue;
            }
            Result one = parseLine(line);
            if (!one.isOk()) {
                // 严格模式：不符合的行整行舍弃，但保留其余合格行，避免一条坏行毁掉整批
                badLines.add(new BadLine(i + 1, line, one.getReason()));
                continue;
            }
            if (one.isAdjusted() && adjustReason == null) {
                adjustReason = "line#" + (i + 1) + ":" + one.getAdjustReason();
            }
            rows.addAll(one.getRows());
        }
        if (rows.isEmpty()) {
            String reason = badLines.isEmpty() ? "empty-payload" : "line#" + badLines.get(0).lineNo + ":"
                    + badLines.get(0).reason;
            return Result.fail(reason);
        }
        return adjustReason == null ? Result.ok(rows, badLines) : Result.okAdjusted(rows, adjustReason, badLines);
    }

    private Result parseLine(String line) {
        if (mode == Mode.RAW) {
            // 原样透传：不切分、不校验列数、不补位，行内 TAB 与空字段原封不动
            return Result.single(line);
        }
        boolean looksJson = line.trim().startsWith("{");
        if (mode == Mode.JSON || (mode == Mode.AUTO && looksJson)) {
            String row = jsonRow(line);
            return row == null ? Result.fail("json-invalid") : Result.single(row);
        }
        return delimitedRow(line);
    }

    // ------------------------------------------------------------------ JSON

    private String jsonRow(String payload) {
        String[] values = new String[DataSchema.SIZE];
        try (JsonParser parser = jsonFactory.createParser(payload)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return null;
            }
            while (true) {
                JsonToken token = parser.nextToken();
                if (token != JsonToken.FIELD_NAME) {
                    // END_OBJECT 或流结束
                    break;
                }
                Integer index = DataSchema.indexOf(parser.getCurrentName());
                JsonToken valueToken = parser.nextToken();
                if (index == null) {
                    // 清单外的字段（含嵌套结构）直接跳过，保持流位置正确
                    if (valueToken == JsonToken.START_OBJECT || valueToken == JsonToken.START_ARRAY) {
                        parser.skipChildren();
                    }
                    continue;
                }
                String value = scalar(parser, valueToken);
                if (value != null) {
                    // 同名字段重复出现时以最后一个为准，与常见 JSON 解析器行为一致
                    values[index] = value;
                }
            }
        } catch (IOException e) {
            // 截断/非法 JSON：整条转入 reject，避免写出列数不全的行
            return null;
        }
        return join(values);
    }

    // ------------------------------------------------------------- 分隔符文本

    private Result delimitedRow(String line) {
        // 只去掉行尾 \r，不能整体 trim：行首/行尾的空字段靠分隔符占位，trim 会丢列
        String[] tokens = separator.split(line, -1);
        if (tokens.length == 1) {
            // 兜底一：上游把 TAB 当字面量 \"\\t\"（反斜杠+t）发出，按真实 TAB 切不开
            if (line.contains("\\t")) {
                tokens = line.split("\\\\t", -1);
                warnOnce("literal-tab", "报文里是字面量 \\t（反斜杠+t）而不是真实制表符，已自动按字面量切分；"
                        + "建议上游修正为真实 TAB");
            } else {
                // 兜底二：既无 TAB 也无字面量 \t —— 分隔符与配置不符，明确告警（否则会写出一整列原文 + 30 个 \\N）
                warnOnce("no-delimiter", "报文里找不到制表符，按 TAB 切分只得到 1 列；"
                        + "请确认 file.delimiter-regex 与上游实际分隔符一致（可用 bin/dump-message.sh 查看原始报文）");
            }
        }
        // 分隔符之间的空字段保持空字符串（下游入库时按空值处理，写 \N 会导致入库异常）
        // 注意：此处不做 null 转换，join 里 null 与 "" 都输出空字符串
        if (columnMapping != null) {
            return mappedRow(tokens);
        }
        if (expectedColumns <= 0) {
            // 自适应：不限制列数，报文有多少列就输出多少列
            return Result.single(join(tokens));
        }
        if (tokens.length == expectedColumns) {
            return Result.single(join(tokens));
        }
        if (!strictFieldCount) {
            // 宽容模式：按期望列数补齐/截断，并标记「已调整」供审计留痕
            String[] normalized = new String[expectedColumns];
            System.arraycopy(tokens, 0, normalized, 0, Math.min(tokens.length, expectedColumns));
            String flag = tokens.length < expectedColumns ? "padded" : "truncated";
            return Result.adjusted(join(normalized), flag + "(expected=" + expectedColumns
                    + ",actual=" + tokens.length + ")");
        }
        // 严格模式：数量不符宁可隔离也不写错位数据，并把实际字段数记进 reason 供现场校准
        return Result.fail("field-count-mismatch(expected=" + expectedColumns + ",actual=" + tokens.length + ")");
    }

    // ------------------------------------------------------------------ 工具

    /** 按显式列映射装载：源列顺序与清单顺序不一致时使用，未映射到的清单列写 \N */
    private Result mappedRow(String[] tokens) {
        boolean mismatch = tokens.length != columnMapping.length;
        if (mismatch && strictFieldCount) {
            return Result.fail("field-count-mismatch(expected=" + columnMapping.length
                    + ",actual=" + tokens.length + ")");
        }
        String[] values = new String[DataSchema.SIZE];
        int n = Math.min(tokens.length, columnMapping.length);
        for (int i = 0; i < n; i++) {
            values[columnMapping[i] - 1] = tokens[i];
        }
        String row = join(values);
        if (!mismatch) {
            return Result.single(row);
        }
        String flag = tokens.length < columnMapping.length ? "padded" : "truncated";
        return Result.adjusted(row, flag + "(expected=" + columnMapping.length
                + ",actual=" + tokens.length + ")");
    }

    /** 解析列映射配置："19,20,21,25" → int[]{19,20,21,25}，越界/重复/非数字启动即失败 */
    private static int[] parseMapping(String spec, int targetSize) {
        if (spec == null || spec.trim().isEmpty()) {
            return null;
        }
        String[] parts = spec.split(",");
        int[] mapping = new int[parts.length];
        boolean[] used = new boolean[targetSize];
        for (int i = 0; i < parts.length; i++) {
            int target;
            try {
                target = Integer.parseInt(parts[i].trim());
            } catch (NumberFormatException e) {
                throw new IllegalStateException("file.column-mapping 第 " + (i + 1) + " 项不是数字: " + parts[i]);
            }
            if (target < 1 || target > targetSize) {
                throw new IllegalStateException("file.column-mapping 第 " + (i + 1) + " 项越界（1~"
                        + targetSize + "）: " + target);
            }
            if (used[target - 1]) {
                throw new IllegalStateException("file.column-mapping 目标列重复: " + target);
            }
            used[target - 1] = true;
            mapping[i] = target;
        }
        return mapping;
    }

    private static String join(String[] values) {
        StringBuilder sb = new StringBuilder(256);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(DELIMITER);
            }
            // 缺失/空字段一律输出空字符串（不写 \N，避免下游入库报错）
            sb.append(values[i] == null ? "" : clean(values[i]));
        }
        return sb.toString();
    }

    private static Mode parseMode(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return Mode.AUTO;
        }
        try {
            return Mode.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("file.parse-mode 非法: " + raw + "（可选 AUTO / JSON / DELIMITER）");
        }
    }

    /** 取标量值的文本形式；对象/数组视为缺失（已被 skipChildren 跳过） */
    private static String scalar(JsonParser parser, JsonToken token) throws IOException {
        if (token == null) {
            return null;
        }
        switch (token) {
            case VALUE_STRING:
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
            case VALUE_TRUE:
            case VALUE_FALSE:
                // getText 对数字返回原始字面量，不会因 double 转换丢掉长整型精度
                return parser.getText();
            case START_OBJECT:
            case START_ARRAY:
                parser.skipChildren();
                return null;
            default:
                return null;
        }
    }

    /** 制表符与换行无法转义，统一替换为空格以保证列不错位 */
    private static String clean(String value) {
        return value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    private static String stripLineEnd(String line) {
        int end = line.length();
        while (end > 0 && (line.charAt(end - 1) == '\r')) {
            end--;
        }
        return end == line.length() ? line : line.substring(0, end);
    }

    /** 同类问题只告警一次，避免每批刷屏 */
    private void warnOnce(String key, String message) {
        if (warned.add(key)) {
            log.warn(message);
        }
    }

    /** 渲染结果：成功带多行内容（可能含舍弃行/补位标记），全部失败时带原因 */
    static final class Result {

        private final List<String> rows;
        private final List<BadLine> badLines;
        private final String reason;
        private final String adjustReason;

        private Result(List<String> rows, List<BadLine> badLines, String reason, String adjustReason) {
            this.rows = rows;
            this.badLines = badLines;
            this.reason = reason;
            this.adjustReason = adjustReason;
        }

        static Result single(String row) {
            return new Result(Collections.singletonList(row), Collections.<BadLine>emptyList(), null, null);
        }

        static Result adjusted(String row, String adjustReason) {
            return new Result(Collections.singletonList(row), Collections.<BadLine>emptyList(), null, adjustReason);
        }

        static Result ok(List<String> rows, List<BadLine> badLines) {
            return new Result(rows, badLines, null, null);
        }

        static Result okAdjusted(List<String> rows, String adjustReason, List<BadLine> badLines) {
            return new Result(rows, badLines, null, adjustReason);
        }

        static Result fail(String reason) {
            return new Result(Collections.<String>emptyList(), Collections.<BadLine>emptyList(), reason, null);
        }

        boolean isOk() {
            return reason == null;
        }

        /** 是否发生过补位/截断（主文件照常写入，但需要把原始报文留痕） */
        boolean isAdjusted() {
            return adjustReason != null;
        }

        String getAdjustReason() {
            return adjustReason == null ? "" : adjustReason;
        }

        List<String> getRows() {
            return rows;
        }

        /** 被舍弃的行（严格模式下字段数不符），调用方需按行留存原始内容 */
        List<BadLine> getBadLines() {
            return badLines;
        }

        String getRow() {
            return rows.isEmpty() ? null : rows.get(0);
        }

        String getReason() {
            return reason == null ? "" : reason;
        }
    }

    /** 被舍弃的行：行号 + 原始内容 + 原因 */
    static final class BadLine {

        private final int lineNo;
        private final String content;
        private final String reason;

        BadLine(int lineNo, String content, String reason) {
            this.lineNo = lineNo;
            this.content = content;
            this.reason = reason;
        }

        int getLineNo() {
            return lineNo;
        }

        String getContent() {
            return content;
        }

        String getReason() {
            return reason;
        }
    }
}
