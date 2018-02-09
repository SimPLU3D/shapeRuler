package rules

import (
	"database/sql"
	"fmt"
	cfg "shapeRuler/config"
	"shapeRuler/shapeUtils"
	//"strconv"

	//"github.com/jonas-p/go-shp"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func PgGetRulesFor(thisRow *shapeUtils.ShapeRow, c *cfg.ConfJson, db *sql.DB /*, dir string*/) (string, error) {
	idpar := thisRow.Key
	stmt := "SELECT * FROM regles_plu WHERE " + c.Rules.Key + " = $1 LIMIT 1"
	rows, err := db.Query(stmt, idpar)
	if err != nil {
		fmt.Println("error accessing db" + err.Error())
		return "", err
	}
	result := make([]string, len(c.Rules.Fields))
	r := make(map[string]string)
	noMatch := true
	for rows.Next() {
		noMatch = false
		ptrs := make([]interface{}, len(c.Rules.Fields))
		for i := 0; i < len(c.Rules.Fields); i++ {
			ptrs[i] = &result[i]
		}
		err := rows.Scan(ptrs...)
		if err != nil {
			fmt.Println("error reading row ", err.Error())
			return "", err
		}
		for i := 0; i < len(c.Rules.Fields); i++ {
			r[c.Rules.Fields[i]] = result[i]
		}
	}
	thisRow.Rules = r
	var log string
	if noMatch {
		log = idpar
	}
	return log, nil
}
