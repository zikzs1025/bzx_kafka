package com.example.kafkaloader.file;

import com.example.kafkaloader.config.FilePipelineProperties;
import com.example.kafkaloader.model.KafkaMessageRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 校验「消息体 -> 31 列」的映射规则：顺序、空值占位、脏字符、异常报文。
 */
class RecordRendererTest {

    private final RecordRenderer renderer = new RecordRenderer();

    private KafkaMessageRecord record(String payload) {
        return new KafkaMessageRecord("DISTRIBUTE_SQAJXYRXX_TO", 0, 100L, "k1", payload, "",
                1760000000000L, LocalDateTime.of(2026, 10, 23, 9, 0, 0));
    }

    private String[] columns(String payload) {
        String row = renderer.csvRow(record(payload));
        assertNotNull(row, "payload 应能正常映射: " + payload);
        String[] cols = row.split("\t", -1);
        assertEquals(DataSchema.SIZE, cols.length, "列数必须等于字段清单数量");
        return cols;
    }

    @Test
    void shouldHaveThirtyOneFieldsMatchingChecklist() {
        assertEquals(31, DataSchema.SIZE);
        assertEquals("md_id", DataSchema.FIELDS.get(0).getName());
        assertEquals("capture_time", DataSchema.FIELDS.get(4).getName());
        assertEquals(DataSchema.Type.BIGINT, DataSchema.FIELDS.get(4).getType());
        assertEquals("zjhm", DataSchema.FIELDS.get(18).getName());
        assertEquals("xm", DataSchema.FIELDS.get(19).getName());
        assertEquals(DataSchema.Type.DATETIME, DataSchema.FIELDS.get(24).getType());
        assertEquals("synchro_time", DataSchema.FIELDS.get(30).getName());
    }

    @Test
    void shouldMapFieldsInSchemaOrderRegardlessOfPayloadOrder() {
        // 故意打乱顺序，并在中间插入清单外的字段
        String payload = "{\"synchro_time\":\"2026-10-23 09:00:00\",\"xm\":\"张三\","
                + "\"extra_field\":\"忽略我\",\"md_id\":\"M001\",\"capture_time\":1760000000000,"
                + "\"zjhm\":\"110101199001011234\"}";

        String[] cols = columns(payload);
        assertEquals("M001", cols[0], "第 1 列必须是 md_id");
        assertEquals("1760000000000", cols[4], "BIGINT 应原样输出，不能走 double 丢精度");
        assertEquals("110101199001011234", cols[18]);
        assertEquals("张三", cols[19]);
        assertEquals("2026-10-23 09:00:00", cols[30]);
    }

    @Test
    void shouldUseNullPlaceholderForMissingAndNullValues() {
        String[] cols = columns("{\"md_id\":\"M002\",\"remark\":null}");
        assertEquals("M002", cols[0]);
        assertEquals("", cols[1], "缺失字段输出空字符串");
        assertEquals("", cols[11], "显式 null 输出空字符串");
    }

    @Test
    void shouldSkipUnknownFieldsIncludingNestedObjects() {
        String payload = "{\"md_id\":\"M003\",\"nested\":{\"a\":{\"b\":1},\"list\":[1,2,3]},"
                + "\"xm\":\"李四\",\"arr\":[{\"c\":2}]}";
        String[] cols = columns(payload);
        assertEquals("M003", cols[0]);
        assertEquals("李四", cols[19], "嵌套字段必须被正确跳过，不能打乱后续取值");
    }

    @Test
    void shouldReplaceTabsAndNewlinesInsideValues() {
        String[] cols = columns("{\"md_id\":\"M004\",\"remark\":\"a\\tb\\nc\\rd\"}");
        assertEquals("a b c d", cols[11], "制表符/换行必须替换为空格，否则整行列错位");
    }

    @Test
    void shouldKeepNumericAndBooleanLiterals() {
        String[] cols = columns("{\"capture_time\":1760000000123,\"saved_time\":1.5,"
                + "\"sensitive_level\":true}");
        assertEquals("1760000000123", cols[4]);
        assertEquals("1.5", cols[10]);
        assertEquals("true", cols[9]);
    }

    @Test
    void shouldHandleEmptyObjectAsAllNullRow() {
        String[] cols = columns("{}");
        for (String col : cols) {
            assertEquals("", col, "空对象应整行为空字符串，而不是丢行");
        }
    }

    @Test
    void shouldRejectPayloadThatIsNotJsonObject() {
        assertNull(renderer.csvRow(record("这是一段纯文本")), "非 JSON 必须拒绝，交由隔离区保存");
        assertNull(renderer.csvRow(record("[1,2,3]")), "数组不是对象，无法按字段取值");
        assertNull(renderer.csvRow(record("{\"md_id\":\"M005\"")), "截断 JSON 必须拒绝");
        assertNull(renderer.csvRow(record(null)));
        assertNull(renderer.csvRow(record("   ")));
        assertNull(renderer.csvRow(record("")));
    }

    @Test
    void shouldTolerateDuplicateKeysUsingLastValue() {
        String[] cols = columns("{\"md_id\":\"first\",\"md_id\":\"last\"}");
        assertEquals("last", cols[0]);
    }

    @Test
    void shouldProduceRowWithNoTrailingDelimiter() {
        String row = renderer.csvRow(record("{\"md_id\":\"M006\"}"));
        assertNotNull(row);
        assertTrue(row.endsWith("\t"), "最后一列是 synchro_time，未提供时应为空（行尾仍是分隔符）");
        assertEquals(30, row.split("\t", -1).length - 1, "31 列应正好 30 个分隔符");
    }

    // ------------------------------------------------------- 分隔符文本报文（现场实际形态）

    /** 构造 31 个位置字段的 TAB 分隔报文，值形如 v1..v31 */
    private String delimited(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            if (i > 1) {
                sb.append('\t');
            }
            sb.append('v').append(i);
        }
        return sb.toString();
    }

    @Test
    void shouldParseTabDelimitedPayloadByPosition() {
        String[] cols = columns(delimited(31));
        assertEquals("v1", cols[0], "第 1 列对应 md_id");
        assertEquals("v5", cols[4], "第 5 列对应 capture_time");
        assertEquals("v19", cols[18], "第 19 列对应 zjhm");
        assertEquals("v20", cols[19], "第 20 列对应 xm");
        assertEquals("v31", cols[30], "第 31 列对应 synchro_time");
    }

    @Test
    void shouldKeepEmptyFieldsByConsecutiveTabs() {
        // 空字段靠连续 TAB 占位，切分后必须保留为空列（而不是左移合并）
        String payload = "v1\t\tv3\tv4\tv5\tv6\tv7\tv8\tv9\tv10\tv11\tv12\tv13\tv14\tv15\tv16"
                + "\t\tv18\tv19\tv20\tv21\tv22\tv23\tv24\tv25\tv26\tv27\tv28\tv29\tv30\tv31";
        String[] cols = columns(payload);
        assertEquals(31, cols.length);
        assertEquals("", cols[1], "空字段应为空字符串而不是把后面的值左移");
        assertEquals("v3", cols[2]);
        assertEquals("", cols[16], "第 17 列空");
        assertEquals("v18", cols[17]);
    }

    @Test
    void shouldProduceMultipleRowsFromMultiLinePayload() {
        String payload = delimited(31) + "\r\n" + delimited(31);
        RecordRenderer.Result result = renderer.parse(record(payload));
        assertTrue(result.isOk(), "多行报文应能解析");
        assertEquals(2, result.getRows().size(), "一条消息含两行，应产出两行");
        assertTrue(result.getRows().get(0).startsWith("v1\tv2"));
    }

    @Test
    void shouldKeepGoodLinesAndRecordBadLineSeparately() {
        String payload = delimited(31) + "\n" + "只有一列";
        RecordRenderer.Result result = renderer.parse(record(payload));
        assertTrue(result.isOk(), "整行舍弃策略：合格行照常输出");
        assertEquals(1, result.getRows().size(), "只剩 1 行合格数据");
        assertEquals(1, result.getBadLines().size(), "坏行单独留存");
        assertEquals(2, result.getBadLines().get(0).getLineNo(), "应指出坏行行号");
        assertEquals("只有一列", result.getBadLines().get(0).getContent(), "留存的是该行原文");
        assertTrue(result.getBadLines().get(0).getReason().startsWith("field-count-mismatch"));
    }

    @Test
    void shouldFailWholeMessageWhenAllLinesBad() {
        RecordRenderer.Result result = renderer.parse(record("只有一列"));
        assertFalse(result.isOk(), "整条都坏时直接隔离");
        assertEquals("line#1:field-count-mismatch(expected=31,actual=1)", result.getReason());
    }

    @Test
    void shouldReportFieldCountMismatchWithActualCount() {
        RecordRenderer.Result result = renderer.parse(record(delimited(28)));
        assertFalse(result.isOk(), "字段数不足且严格模式下必须隔离，避免写出错位数据");
        assertEquals("line#1:field-count-mismatch(expected=31,actual=28)", result.getReason());
    }

    @Test
    void shouldReportEmptyAndInvalidJsonReasons() {
        assertEquals("empty-payload", renderer.parse(record("   ")).getReason());
        assertEquals("line#1:json-invalid", renderer.parse(record("{\"md_id\":\"x\"")).getReason());
    }

    @Test
    void shouldPadMissingFieldsWhenStrictDisabled() {
        RecordRenderer lenient = new RecordRenderer(RecordRenderer.Mode.AUTO, "\t", false);
        RecordRenderer.Result result = lenient.parse(record(delimited(3)));
        assertTrue(result.isOk());
        String[] cols = result.getRows().get(0).split("\t", -1);
        assertEquals(31, cols.length);
        assertEquals("v1", cols[0]);
        assertEquals("", cols[3], "不足部分右侧补空字符串");
    }

    @Test
    void shouldForceJsonModeAndRejectPlainText() {
        RecordRenderer jsonOnly = new RecordRenderer(RecordRenderer.Mode.JSON, "\t", true);
        assertFalse(jsonOnly.parse(record(delimited(31))).isOk(), "JSON 模式下非 JSON 行必须隔离");
        assertTrue(jsonOnly.parse(record("{\"md_id\":\"M\"}")).isOk());
    }

    // ------------------------------------------------------------- 列映射（上游列序≠清单列序）

    @Test
    void shouldMapSourceColumnsToChecklistPositions() {
        // 报文 4 列：zjhm(清单19) / xm(20) / xbdm(21) / csrq(25)
        RecordRenderer mapped = new RecordRenderer(RecordRenderer.Mode.AUTO, "\t", true,
                new int[]{19, 20, 21, 25});
        String[] cols = mapped.parse(record("440101199001011234\t张三\t1\t1990-01-01"))
                .getRows().get(0).split("\t", -1);
        assertEquals(31, cols.length);
        assertEquals("", cols[0], "未映射的列为空字符串");
        assertEquals("440101199001011234", cols[18]);
        assertEquals("张三", cols[19]);
        assertEquals("1", cols[20]);
        assertEquals("1990-01-01", cols[24]);
    }

    @Test
    void shouldRejectWhenMappedRowColumnCountDiffers() {
        RecordRenderer mapped = new RecordRenderer(RecordRenderer.Mode.AUTO, "\t", true,
                new int[]{19, 20, 21, 25});
        RecordRenderer.Result result = mapped.parse(record("只有一列"));
        assertFalse(result.isOk());
        assertEquals("line#1:field-count-mismatch(expected=4,actual=1)", result.getReason());
    }

    @Test
    void shouldFailFastOnInvalidColumnMapping() {
        assertThrows(IllegalStateException.class, () -> new RecordRenderer(new FilePipelineProperties() {{
            setColumnMapping("19,99");
        }}));
        assertThrows(IllegalStateException.class, () -> new RecordRenderer(new FilePipelineProperties() {{
            setColumnMapping("19,19");
        }}));
        assertThrows(IllegalStateException.class, () -> new RecordRenderer(new FilePipelineProperties() {{
            setColumnMapping("19,x");
        }}));
    }

    // ------------------------------------------------------------- 宽容模式（主文件恒 31 列）

    @Test
    void lenientModeShouldPadAndMarkAdjusted() {
        RecordRenderer lenient = new RecordRenderer(RecordRenderer.Mode.AUTO, "\t", false);
        RecordRenderer.Result result = lenient.parse(record(delimited(12)));
        assertTrue(result.isOk(), "宽容模式下不隔离，照常写主文件");
        assertTrue(result.isAdjusted(), "应标记为已补位，供原始报文留痕");
        assertEquals("line#1:padded(expected=31,actual=12)", result.getAdjustReason());
        assertEquals(31, result.getRows().get(0).split("\t", -1).length, "主文件必须恒为 31 列");
    }

    @Test
    void lenientModeShouldTruncateExtraColumns() {
        RecordRenderer lenient = new RecordRenderer(RecordRenderer.Mode.AUTO, "\t", false);
        RecordRenderer.Result result = lenient.parse(record(delimited(35)));
        assertTrue(result.isOk());
        assertEquals("line#1:truncated(expected=31,actual=35)", result.getAdjustReason());
        assertEquals(31, result.getRows().get(0).split("\t", -1).length);
    }

    @Test
    void exactThirtyOneColumnsShouldNotBeMarkedAdjusted() {
        RecordRenderer lenient = new RecordRenderer(RecordRenderer.Mode.AUTO, "\t", false);
        RecordRenderer.Result result = lenient.parse(record(delimited(31)));
        assertTrue(result.isOk());
        assertFalse(result.isAdjusted(), "列数正好 31 不应被判为补位");
    }

    // ------------------------------------------------- 分隔符不匹配的兜底与告警

    @Test
    void shouldFallbackToLiteralBackslashTDelimiter() {
        // 上游把 TAB 当字面量 "\\t" 发出（反斜杠 + t 两个字符）
        String payload = delimited(31).replace("\t", "\\t");
        RecordRenderer.Result result = renderer.parse(record(payload));
        assertTrue(result.isOk(), "字面量 \\t 应能自动兜底切分");
        String[] cols = result.getRows().get(0).split("\t", -1);
        assertEquals(31, cols.length);
        assertEquals("v1", cols[0]);
        assertEquals("v31", cols[30]);
    }

    @Test
    void shouldRejectSpaceDelimitedPayloadWhenDelimiterIsTab() {
        // 纯空格分隔、且无字面量 \t：严格模式必须整行舍弃，绝不写出一整列原文 + 30 个 \N
        String spacePayload = delimited(31).replace("\t", " ");
        RecordRenderer.Result result = renderer.parse(record(spacePayload));
        assertFalse(result.isOk(), "分隔符不匹配时应隔离而不是补位");
        assertTrue(result.getReason().contains("field-count-mismatch"));
    }

    // ------------------------------------------------------------- 自适应列数（期望列数=0）

    @Test
    void adaptiveModeShouldOutputActualColumnCount() {
        RecordRenderer adaptive = new RecordRenderer(RecordRenderer.Mode.AUTO, "\t", true, 0, null);
        RecordRenderer.Result result = adaptive.parse(record(delimited(12)));
        assertTrue(result.isOk(), "自适应模式不校验列数");
        String[] cols = result.getRows().get(0).split("\t", -1);
        assertEquals(12, cols.length, "报文几列就输出几列");
        assertEquals("v12", cols[11]);
    }

    @Test
    void adaptiveModeShouldKeepEmptyFieldsAsNull() {
        RecordRenderer adaptive = new RecordRenderer(RecordRenderer.Mode.AUTO, "\t", true, 0, null);
        RecordRenderer.Result result = adaptive.parse(record("a\t\tc"));
        String[] cols = result.getRows().get(0).split("\t", -1);
        assertEquals(3, cols.length);
        assertEquals("", cols[1], "空字段输出空字符串");
    }

    // ------------------------------------------------------------- 原样透传（RAW）

    @Test
    void rawModeShouldPassLineThroughUnchanged() {
        RecordRenderer raw = new RecordRenderer(RecordRenderer.Mode.RAW, "\t", true, 0, null);
        String line = "15925898445\t\t\tP42032712151100000100A\t43122199907206112\t唐飞飞";
        RecordRenderer.Result result = raw.parse(record(line));
        assertTrue(result.isOk(), "原样透传不做任何校验");
        assertEquals(line, result.getRows().get(0), "TAB 与空字段必须原封不动");
    }

    @Test
    void rawModeShouldSplitMultiLineIntoRows() {
        RecordRenderer raw = new RecordRenderer(RecordRenderer.Mode.RAW, "\t", true, 0, null);
        RecordRenderer.Result result = raw.parse(record("a\tb\nc\td\n"));
        assertEquals(2, result.getRows().size(), "多行报文按行拆成多行，尾部空行忽略");
        assertEquals("a\tb", result.getRows().get(0));
        assertEquals("c\td", result.getRows().get(1));
    }
}
