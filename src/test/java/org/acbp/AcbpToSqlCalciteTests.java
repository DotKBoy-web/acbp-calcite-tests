package org.acbp;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

public class AcbpToSqlCalciteTests {
  private static boolean astEquals(String a, String b) {
    try {
      SqlParser.Config cfg = SqlParser.config().withCaseSensitive(false);
      SqlNode na = SqlParser.create(a, cfg).parseQuery();
      SqlNode nb = SqlParser.create(b, cfg).parseQuery();
      return na.equalsDeep(nb, org.apache.calcite.util.Litmus.IGNORE);
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testA_postgres_via_calcite() {
    String dsl = """
      model hl7_v1 {
        from hl7_messages
        time_column event_ts
        category message_type  := enum('ADT','ORM','ORU')
        category trigger_event := enum('A01','A04','A03','O01','R01')
        category patient_class := enum('E','I','O')
        flag is_admission := message_type = 'ADT' and trigger_event in ('A01','A04')
        flag is_emergency := patient_class = 'E'
        decision action_id {
          when is_admission and is_emergency -> 2
          else -> 1
        }
      }""";
    String sql = CalciteBackedAcbpToSql.acbpToSql(dsl, "decision_space_hl7", Dialect.POSTGRESQL, Map.of("windowDays", 2));
    String expected = """
      SELECT
        msg_id,
        event_ts,
        CASE
          WHEN (message_type = 'ADT' AND trigger_event IN ('A01','A04') AND patient_class = 'E') THEN 2
          ELSE 1
        END AS action_id
      FROM hl7_messages
      WHERE event_ts >= now() - interval '2 days';""";
    assertTrue(astEquals(expected, sql), () -> "\nExpected:\n" + expected + "\nActual:\n" + sql);
  }

  @Test
  void testB_bigquery_via_calcite() {
    String dsl = """
      model hl7_v1 {
        from hl7_messages
        time_column event_ts
        category message_type := enum('ADT','ORM','ORU')
        category obs_abn_flag := enum('H','L','N')
        ref critical_loinc := "ref_critical_loinc"
        flag abnormal_result  := message_type = 'ORU' and obs_abn_flag in ('H','L')
        flag critical_analyte := loinc_code in (select code from ref_critical_loinc)
        decision action_id {
          when abnormal_result and critical_analyte -> 3
          when abnormal_result -> 2
          else -> 1
        }
      }""";
    String sql = CalciteBackedAcbpToSql.acbpToSql(dsl, "decision_space_hl7", Dialect.BIGQUERY, Map.of("windowDays", 2));
    String expected = """
      SELECT
        msg_id,
        event_ts,
        CASE
          WHEN (message_type = 'ORU' AND obs_abn_flag IN ('H','L')
                AND loinc_code IN (SELECT code FROM ref_critical_loinc)) THEN 3
          WHEN (message_type = 'ORU' AND obs_abn_flag IN ('H','L')) THEN 2
          ELSE 1
        END AS action_id
      FROM hl7_messages
      WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY);""";
    assertTrue(astEquals(expected, sql), () -> "\nExpected:\n" + expected + "\nActual:\n" + sql);
  }

  @Test
  void testC_clickhouse_via_calcite() {
    String dsl = """
      model hl7_v1 {
        from hl7_messages
        time_column event_ts
        category message_type  := enum('ADT','ORM','ORU')
        category order_priority := enum('STAT','ROUTINE')
        category obs_abn_flag := enum('H','L','N')
        flag is_stat_order   := message_type = 'ORM' and order_priority = 'STAT'
        flag abnormal_result := obs_abn_flag in ('H','L')
        decision action_id {
          when is_stat_order and abnormal_result -> 3
          when is_stat_order -> 2
          else -> 1
        }
      }""";
    String sql = CalciteBackedAcbpToSql.acbpToSql(dsl, "decision_space_hl7", Dialect.CLICKHOUSE, Map.of("windowDays", 2));
    String expected = """
      SELECT
        msg_id,
        event_ts,
        CASE
          WHEN (message_type = 'ORM' AND order_priority = 'STAT'
                AND obs_abn_flag IN ('H','L')) THEN 3
          WHEN (message_type = 'ORM' AND order_priority = 'STAT') THEN 2
          ELSE 1
        END AS action_id
      FROM hl7_messages
      WHERE event_ts >= now() - INTERVAL 2 DAY;""";
    assertTrue(astEquals(expected, sql), () -> "\nExpected:\n" + expected + "\nActual:\n" + sql);
  }

  @Test
  void testD_postgres_table_driven_via_calcite() {
    String dsl = """
      model hl7_v1 {
        from hl7_messages
        time_column event_ts
        category message_type  := enum(select code from ref_message_type)
        category trigger_event := enum(select code from ref_trigger_event)
        category patient_class := enum(select code from ref_patient_class)
        ref emergency_classes := "ref_patient_class_emergency"
        flag is_emergency := patient_class in (select code from ref_patient_class_emergency)
        flag is_admission := message_type = 'ADT' and trigger_event in ('A01','A04')
        decision action_id {
          when is_admission and is_emergency -> 2
          else -> 1
        }
      }""";
    String sql = CalciteBackedAcbpToSql.acbpToSql(dsl, "decision_space_hl7", Dialect.POSTGRESQL, Map.of("windowDays", 2));
    String expected = """
      SELECT
        msg_id,
        event_ts,
        CASE
          WHEN (message_type = 'ADT'
                AND trigger_event IN ('A01','A04')
                AND patient_class IN (SELECT code FROM ref_patient_class_emergency)) THEN 2
          ELSE 1
        END AS action_id
      FROM hl7_messages
      WHERE event_ts >= now() - interval '2 days';""";
    assertTrue(astEquals(expected, sql), () -> "\nExpected:\n" + expected + "\nActual:\n" + sql);
  }
}
