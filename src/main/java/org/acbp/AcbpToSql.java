package org.acbp;

import java.util.Map;
import java.util.regex.Pattern;

/** Minimal, deterministic DSL -> SQL generator for the tests. */
public final class AcbpToSql {

  public static String acbpToSql(String modelDsl, String entryPoint, Dialect dialect, Map<String,Object> options) {
    if (!"decision_space_hl7".equals(entryPoint)) {
      throw new IllegalArgumentException("Unsupported entryPoint: " + entryPoint);
    }
    AcbpModel m = AcbpModel.parse(modelDsl);
    int windowDays = parseWindowDays(options);

    // Build CASE from rules, expanding flag names into predicates (single outer paren only).
    StringBuilder caseExpr = new StringBuilder();
    caseExpr.append("CASE\n");
    for (AcbpModel.Rule r : m.rules) {
      String expanded = expandFlags(r.condition(), m);
      caseExpr.append("          WHEN (").append(expanded).append(") THEN ").append(r.action()).append("\n");
    }
    caseExpr.append("          ELSE ").append(m.defaultAction != null ? m.defaultAction : 0).append("\n");
    caseExpr.append("        END AS action_id");

    String where = renderWindow(m.timeColumn, windowDays, dialect);

    // Final SELECT
    return "SELECT\n" +
           "  msg_id,\n" +
           "  " + m.timeColumn + ",\n" +
           "  " + caseExpr + "\n" +
           "FROM " + m.fromTable + "\n" +
           "WHERE " + where + ";";
  }

  private static int parseWindowDays(Map<String,Object> opts) {
    Object v = opts == null ? null : opts.get("windowDays");
    if (v instanceof Number n) return n.intValue();
    if (v instanceof String s) return Integer.parseInt(s);
    return 2; // default like in tests
  }

  private static String expandFlags(String condition, AcbpModel m) {
    String out = condition;
    for (var e : m.flags.entrySet()) {
      String name = e.getKey();
      String pred = e.getValue().trim(); // no extra parens around each flag
      out = out.replaceAll("\\b" + Pattern.quote(name) + "\\b", pred);
    }
    // Normalize boolean operators to upper-case for stable output
    out = out.replaceAll("\\band\\b", "AND");
    out = out.replaceAll("\\bor\\b", "OR");
    out = out.replaceAll("\\bnot\\b", "NOT");
    return out;
  }

  private static String renderWindow(String timeCol, int days, Dialect d) {
    return switch (d) {
      case POSTGRESQL -> timeCol + " >= now() - interval '" + days + " days'";
      case BIGQUERY   -> timeCol + " >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL " + days + " DAY)";
      case CLICKHOUSE -> timeCol + " >= now() - INTERVAL " + days + " DAY";
    };
  }
}
