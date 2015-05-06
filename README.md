A small program that records some useful MySQL metrics. Currently, the contents
of `information_schema.INNODB_METRICS` and `information_schema.GLOBAL_STATUS` is
collected. The output is stored an SQLite database called `./current.db`.
Optionally, the metrics can also be printed in JSON on stdout.

Example of how to extract a single metric (uptime):

```bash
sqlite3 -csv current.db '
  SELECT datetime(ts, "unixepoch", "localtime"),value
  FROM Measurement WHERE id = (
    SELECT id FROM Metric WHERE name = "status.uptime"
  )'
```
