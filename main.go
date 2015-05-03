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
)

var (
	dsn   = flag.String("dsn", "root@unix(/var/run/mysqld/mysqld.sock)/", "Data Source Name that points to the MySQL server")
	sleep = flag.Duration("sleep", time.Second, "How long to sleep between two consecutive polls")
)

type entry struct {
	Name      string
	SubSystem string
	Type      string
	Count     int64
	Timestamp time.Time
}

func fatal(err error) {
	log.Fatalf("+%v", err)
}

func innodbMetrics(db *sql.DB) ([]entry, error) {
	query := `
SELECT
  NAME, SUBSYSTEM, TYPE,
  COUNT,
  TIMESTAMPADD(SECOND, TIME_ELAPSED, TIME_ENABLED) TS
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
		err := rows.Scan(&e.Name, &e.SubSystem, &e.Type, &e.Count, &e.Timestamp)
		if err != nil {
			return nil, err
		}
		m = append(m, e)
	}

	return m, nil
}

func main() {
	flag.Parse()
	log.Printf("dsn: %v", *dsn)
	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	for {
		m, err := innodbMetrics(db)
		if err != nil {
			fatal(err)
		}
		b, _ := json.Marshal(m)
		fmt.Println(bytes.NewBuffer(b).String())
		time.Sleep(*sleep)
	}
}
