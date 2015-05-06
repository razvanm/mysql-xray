package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

var (
	dsn        = flag.String("dsn", "root@unix(/var/run/mysqld/mysqld.sock)/", "Data Source Name that points to the MySQL server")
	sleep      = flag.Duration("sleep", time.Second, "How long to sleep between two consecutive polls")
	jsonOutput = flag.Bool("json", false, "Also output the metrics in JSON format")
)

const (
	createMetricTable = `
CREATE TABLE IF NOT EXISTS Metric (
  id INTEGER PRIMARY KEY,
  name TEXT
)
`

	createMeasurementTable = `
CREATE TABLE IF NOT EXISTS Measurement (
  ts TEXT,
  id INTEGER,
  value INTEGER,
  PRIMARY KEY(ts, id)
)
`
)

type entry struct {
	Timestamp time.Time
	Name      string
	Value     int64
}

func fatal(err error) {
	log.Fatalf("+%v", err)
}

func create(db, dblog *sql.DB) error {
	for _, q := range []string{
		createMetricTable,
		createMeasurementTable,
	} {
		_, err := dblog.Exec(q)
		if err != nil {
			return err
		}
	}

	query := `
SELECT CONCAT_WS('.', SUBSYSTEM, NAME, TYPE) name
FROM information_schema.INNODB_METRICS
UNION SELECT CONCAT_WS('.', 'status', LOWER(VARIABLE_NAME)) name
FROM information_schema.GLOBAL_STATUS
`
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := dblog.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO Metric(name) VALUES (?)")
	if err != nil {
		return err
	}

	for rows.Next() {
		var s string
		err = rows.Scan(&s)
		_, err := stmt.Exec(s)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	return err
}

func names(dblog *sql.DB) (map[string]int64, error) {
	m := make(map[string]int64)
	rows, err := dblog.Query("SELECT * FROM Metric")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			return nil, err
		}
		m[name] = id
	}

	return m, nil
}

func measurements(db *sql.DB) ([]entry, error) {
	query := `
SELECT
  NOW() ts,
  CONCAT_WS('.', 'status', LOWER(VARIABLE_NAME)) name,
  VARIABLE_VALUE value
FROM information_schema.GLOBAL_STATUS
UNION SELECT
  TIMESTAMPADD(SECOND, TIME_ELAPSED, TIME_ENABLED) ts,
  CONCAT_WS('.', NAME, SUBSYSTEM, TYPE) name,
  COUNT value
FROM information_schema.INNODB_METRICS
WHERE STATUS = 'enabled'
`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := []entry{}
	for rows.Next() {
		var e entry
		err := rows.Scan(&e.Timestamp, &e.Name, &e.Value)
		if err != nil {
			// A small number of metrics are strings or
			// floats. We are ignoring them for now.
		}
		m = append(m, e)
	}

	return m, nil
}

func save(m []entry, nameToId map[string]int64, dblog *sql.DB) error {
	tx, err := dblog.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO Measurement(ts, id, value) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}

	for _, e := range m {
		_, err := stmt.Exec(e.Timestamp, nameToId[e.Name], e.Value)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	return err
}

func main() {
	flag.Parse()
	log.Printf("dsn: %v", *dsn)

	dblog, err := sql.Open("sqlite3", "./current.db")
	if err != nil {
		fatal(err)
	}
	defer dblog.Close()

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	err = create(db, dblog)
	if err != nil {
		fatal(err)
	}

	nameToId, err := names(dblog)

	n := 0
	for {
		m, err := measurements(db)
		if err != nil {
			fatal(err)
		}

		err = save(m, nameToId, dblog)
		if err != nil {
			fatal(err)
		}

		if *jsonOutput {
			b, _ := json.Marshal(m)
			fmt.Println(bytes.NewBuffer(b).String())
		}

		n += len(m)
		log.Printf("Measurements: %d", n)

		time.Sleep(*sleep)
	}
}
