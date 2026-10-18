# Databricks notebook source
# MAGIC %run ./11_dq_lib

# COMMAND ----------

M = f"{CATALOG}.monitoring"
T_OPS, T_EVENTS, T_ALERTS = f"{M}.incident_ops", f"{M}.incident_events", f"{M}.alerts"
checks = []


def check(name, ok, detail=""):
    checks.append((name, bool(ok), detail))


def q(sql):
    return spark.sql(sql).collect()[0][0]


n_inc = spark.table(T_INCIDENTS).count()
check("there are incidents (run the gate, monitors and recon first)", n_inc > 0, f"{n_inc}")

no_ops = q(f"SELECT count(*) FROM {T_INCIDENTS} i LEFT ANTI JOIN {T_OPS} o ON i.incident_id = o.incident_id")
check("every incident has priority, owner and due time (run 71)", no_ops == 0, f"{no_ops} without")
bad_ops = q(f"SELECT count(*) FROM {T_OPS} WHERE owner IS NULL OR priority IS NULL OR due_at IS NULL")
check("no ops row misses owner, priority or due_at", bad_ops == 0, f"{bad_ops}")
dup_ops = q(f"SELECT count(*) FROM (SELECT incident_id FROM {T_OPS} GROUP BY incident_id HAVING count(*) > 1)")
check("one ops row per incident", dup_ops == 0, f"{dup_ops} duplicated")
late_due = q(f"SELECT count(*) FROM {T_OPS} o JOIN {T_INCIDENTS} i USING (incident_id) WHERE o.due_at <= i.opened_at")
check("due time is after the opening time", late_due == 0, f"{late_due}")

no_ev = 0
for et in ("OPENED", "ASSIGNED"):
    no_ev += q(f"""SELECT count(*) FROM {T_INCIDENTS} i LEFT ANTI JOIN
      (SELECT DISTINCT incident_id FROM {T_EVENTS} WHERE event_type = '{et}') e ON e.incident_id = i.incident_id""")
check("every incident has OPENED and ASSIGNED events", no_ev == 0, f"{no_ev} missing")

p12_no_alert = q(f"""SELECT count(*) FROM {T_INCIDENTS} i JOIN {T_OPS} o USING (incident_id)
  WHERE o.priority IN ('P1','P2') AND NOT EXISTS (SELECT 1 FROM {T_ALERTS} a WHERE a.incident_id = i.incident_id AND a.kind = 'NEW')""")
check("every P1/P2 incident was alerted", p12_no_alert == 0, f"{p12_no_alert} not alerted")
dup_alert = q(f"""SELECT count(*) FROM (SELECT incident_id, kind FROM {T_ALERTS} WHERE kind IN ('NEW','RESOLVED')
  GROUP BY incident_id, kind HAVING count(*) > 1)""")
check("no incident alerted twice for NEW or RESOLVED", dup_alert == 0, f"{dup_alert} duplicates")

res_no_ev = q(f"""SELECT count(*) FROM {T_INCIDENTS} i WHERE i.status = 'RESOLVED'
  AND NOT EXISTS (SELECT 1 FROM {T_EVENTS} e WHERE e.incident_id = i.incident_id AND e.event_type = 'RESOLVED')""")
check("every resolved incident has a RESOLVED event", res_no_ev == 0, f"{res_no_ev} missing")

bad_level = q(f"SELECT count(*) FROM {T_OPS} WHERE escalation_level NOT IN (0,1,2)")
check("escalation level is 0, 1 or 2", bad_level == 0, f"{bad_level}")
ack_noev = q(f"""SELECT count(*) FROM {T_OPS} o WHERE o.acknowledged_at IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM {T_EVENTS} e WHERE e.incident_id = o.incident_id AND e.event_type = 'ACKED')""")
check("every acknowledgement has an ACKED event", ack_noev == 0, f"{ack_noev}")

try:
    k = spark.table(f"{M}.v_incident_kpis").collect()[0].asDict()
    check("views work (v_incident_kpis)", True, str(k))
    board = spark.table(f"{M}.v_incident_board").count()
    check("board has one row per incident", board == n_inc, f"{board} vs {n_inc}")
except Exception as e:
    check("views work (run 73)", False, str(e)[:150])

for name, ok, detail in checks:
    print(("PASS  " if ok else "FAIL  ") + name + (f"   [{detail}]" if detail else ""))
fails = [c for c in checks if not c[1]]
print("\nINCIDENTS OK" if not fails else f"\n{len(fails)} CHECK(S) FAILED")
display(spark.sql(f"""SELECT priority, count(*) AS incidents, sum(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) AS open
                      FROM {M}.v_incident_board GROUP BY priority ORDER BY priority"""))
