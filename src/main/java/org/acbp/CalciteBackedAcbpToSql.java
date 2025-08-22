package org.acbp;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.Map;

/** Calcite-backed SELECT generator (Phase A: WHERE appended via dialect switch). */
public final class CalciteBackedAcbpToSql {
  private CalciteBackedAcbpToSql() {}

  public static String acbpToSql(String modelDsl, String entryPoint, Dialect dialect, Map<String,Object> options) {
    if (!"decision_space_hl7".equals(entryPoint)) {
      throw new IllegalArgumentException("Unsupported entryPoint: " + entryPoint);
    }
    AcbpModel m = AcbpModel.parse(modelDsl);
    int windowDays = parseWindowDays(options);

    // CASE from rules (expand flags)
    StringBuilder caseExpr = new StringBuilder();
    caseExpr.append("CASE\n");
    for (AcbpModel.Rule r : m.rules) {
      String expanded = expandFlags(r.condition(), m);
      caseExpr.append("  WHEN (").append(expanded).append(") THEN ").append(r.action()).append("\n");
    }
    caseExpr.append("  ELSE ").append(m.defaultAction != null ? m.defaultAction : 0).append("\n");
    caseExpr.append("END AS action_id");

    // Core SELECT (no WHERE)
    String coreSelect =
        "SELECT\n" +
        "  msg_id,\n" +
        "  " + m.timeColumn + ",\n" +
        "  " + caseExpr + "\n" +
        "FROM " + m.fromTable;

    String unparsed = unparseWithCalcite(coreSelect, dialect);
    String where = renderWindow(m.timeColumn, windowDays, dialect);
    return unparsed + "\nWHERE " + where + ";";
  }

  private static String expandFlags(String condition, AcbpModel m) {
    String out = condition;
    for (var e : m.flags.entrySet()) {
      String name = e.getKey();
      String pred = e.getValue().trim();
      out = out.replaceAll("\\b" + java.util.regex.Pattern.quote(name) + "\\b", pred);
    }
    out = out.replaceAll("\\band\\b", "AND")
             .replaceAll("\\bor\\b", "OR")
             .replaceAll("\\bnot\\b", "NOT");
    return out;
  }

  private static int parseWindowDays(Map<String,Object> opts) {
    Object v = opts == null ? null : opts.get("windowDays");
    if (v instanceof Number n) return n.intValue();
    if (v instanceof String s) return Integer.parseInt(s);
    return 2;
    }

  private static String renderWindow(String timeCol, int days, Dialect d) {
    return switch (d) {
      case POSTGRESQL -> timeCol + " >= now() - interval '" + days + " days'";
      case BIGQUERY   -> timeCol + " >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL " + days + " DAY)";
      case CLICKHOUSE -> timeCol + " >= now() - INTERVAL " + days + " DAY";
    };
  }

  private static String unparseWithCalcite(String selectSqlNoWhere, Dialect dialect) {
    SqlDialect calciteDialect = switch (dialect) {
      case POSTGRESQL -> org.apache.calcite.sql.dialect.PostgresqlSqlDialect.DEFAULT;
      case BIGQUERY   -> org.apache.calcite.sql.dialect.BigQuerySqlDialect.DEFAULT;
      case CLICKHOUSE -> org.apache.calcite.sql.dialect.ClickHouseSqlDialect.DEFAULT;
    };

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(makeSchemaWithTables())
        .parserConfig(SqlParser.config().withCaseSensitive(false))
        .build();

    try {
      Planner planner = Frameworks.getPlanner(config);
      var parsed = planner.parse(selectSqlNoWhere);
      var validated = planner.validate(parsed);
      RelNode rel = planner.rel(validated).rel;
      var converter = new RelToSqlConverter(calciteDialect);
      SqlNode sqlNode = converter.visitRoot(rel).asStatement();
      SqlPrettyWriter writer = new SqlPrettyWriter(calciteDialect);
      sqlNode.unparse(writer, 0, 0);
      return writer.toString();
    } catch (Exception e) {
      throw new RuntimeException("Calcite unparse failed: " + e.getMessage(), e);
    }
  }

  private static SchemaPlus makeSchemaWithTables() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    SchemaPlus s = root.add("s", new AbstractSchema());
    s.add("hl7_messages", new MessagesTable());
    s.add("ref_critical_loinc", new SingleCodeTable());
    s.add("ref_patient_class_emergency", new SingleCodeTable());
    root.add("hl7_messages", new MessagesTable());
    root.add("ref_critical_loinc", new SingleCodeTable());
    root.add("ref_patient_class_emergency", new SingleCodeTable());
    return root;
  }

  private static final class MessagesTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory f) {
      var b = f.builder();
      b.add("msg_id", SqlTypeName.BIGINT);
      b.add("event_ts", SqlTypeName.TIMESTAMP);
      b.add("message_type", SqlTypeName.VARCHAR);
      b.add("trigger_event", SqlTypeName.VARCHAR);
      b.add("patient_class", SqlTypeName.VARCHAR);
      b.add("order_priority", SqlTypeName.VARCHAR);
      b.add("loinc_code", SqlTypeName.VARCHAR);
      b.add("obs_abn_flag", SqlTypeName.VARCHAR);
      return b.build();
    }
  }

  private static final class SingleCodeTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory f) {
      var b = f.builder();
      b.add("code", SqlTypeName.VARCHAR);
      return b.build();
    }
  }
}
