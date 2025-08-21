package org.acbp;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Minimal ACBP model parsed from the tiny DSL used in tests. */
public final class AcbpModel {
  public final String fromTable;
  public final String timeColumn;
  public final Map<String,String> refs;
  public final Map<String,String> flags;
  public final List<Rule> rules;
  public final Integer defaultAction;

  public record Rule(String condition, int action) {}

  private AcbpModel(String fromTable,
                    String timeColumn,
                    Map<String,String> refs,
                    Map<String,String> flags,
                    List<Rule> rules,
                    Integer defaultAction) {
    this.fromTable = fromTable;
    this.timeColumn = timeColumn;
    this.refs = Collections.unmodifiableMap(refs);
    this.flags = Collections.unmodifiableMap(flags);
    this.rules = Collections.unmodifiableList(rules);
    this.defaultAction = defaultAction;
  }

  /** Parse the very small DSL used in the email tests. */
  public static AcbpModel parse(String dsl) {
    String src = stripComments(dsl);
    String from = findSingle(src, "\\bfrom\\s+([a-zA-Z0-9_]+)");
    String timeCol = findSingle(src, "\\btime_column\\s+([a-zA-Z0-9_]+)");

    Map<String,String> refs = new LinkedHashMap<>();
    // ref name := "table_name"
    Matcher mref = Pattern.compile("\\bref\\s+([a-zA-Z0-9_]+)\\s*:=\\s*\"([^\"]+)\"").matcher(src);
    while (mref.find()) {
      refs.put(mref.group(1), mref.group(2));
    }

    Map<String,String> flags = new LinkedHashMap<>();
    // flag name := <expr>
    Matcher mflag = Pattern.compile("\\bflag\\s+([a-zA-Z0-9_]+)\\s*:=\\s*(.*)").matcher(src);
    while (mflag.find()) {
      String name = mflag.group(1);
      String expr = mflag.group(2).trim();
      // cut off trailing decision block or next statement if matched greedily
      expr = expr.replaceAll("\\s*decision\\s+action_id\\s*\\{.*", "").trim();
      // remove trailing braces if any
      expr = trimRight(expr, '}').trim();
      flags.put(name, expr);
    }

    // decision block: when X -> N ... else -> N
    String decision = findBlock(src, "\\bdecision\\s+action_id\\s*\\{", "}");
    List<Rule> rules = new ArrayList<>();
    Integer defaultAct = null;

    // when <cond> -> <int>
    Matcher mwhen = Pattern.compile("\\bwhen\\s+(.*?)\\s*->\\s*([0-9]+)", Pattern.DOTALL).matcher(decision);
    while (mwhen.find()) {
      String cond = mwhen.group(1).trim();
      int act = Integer.parseInt(mwhen.group(2));
      rules.add(new Rule(cond, act));
    }
    // else -> <int>
    Matcher melse = Pattern.compile("\\belse\\s*->\\s*([0-9]+)").matcher(decision);
    if (melse.find()) {
      defaultAct = Integer.parseInt(melse.group(1));
    }

    return new AcbpModel(from, timeCol, refs, flags, rules, defaultAct);
  }

  private static String stripComments(String s) {
    // remove // comments
    return s.replaceAll("//.*", "");
  }

  private static String findSingle(String s, String regex) {
    Matcher m = Pattern.compile(regex, Pattern.DOTALL).matcher(s);
    if (!m.find()) throw new IllegalArgumentException("Cannot find: " + regex);
    return m.group(1);
  }

  private static String findBlock(String s, String startRegex, String endLiteral) {
    Matcher m = Pattern.compile(startRegex, Pattern.DOTALL).matcher(s);
    if (!m.find()) throw new IllegalArgumentException("Cannot find block start: " + startRegex);
    int start = m.end();
    int depth = 1;
    for (int i = start; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '{') depth++;
      else if (c == '}') {
        depth--;
        if (depth == 0) {
          return s.substring(start, i);
        }
      }
    }
    throw new IllegalArgumentException("Unclosed block for: " + startRegex);
  }

  private static String trimRight(String s, char ch) {
    int end = s.length();
    while (end > 0 && s.charAt(end-1) == ch) end--;
    return s.substring(0, end);
  }
}
