package rules

import (
	"encoding/csv"
	"fmt"
	"io"
	cfg "shapeRuler/config"
	"shapeRuler/shapeUtils"
	//"strconv"

	//"github.com/jonas-p/go-shp"

	"os"
)

func CsvGetRulesFor(thisRow *shapeUtils.ShapeRow, c *cfg.ConfJson, csvFile *os.File, dir string) (string, error) {
	indexId := 0
	log := ""
	idpar := thisRow.Key
	reader := csv.NewReader(csvFile)
	reader.Comma = ';'
	//result := make([]string, len(c.Rules.Fields))
	rowmap := make(map[string]string)

	found := false
	i := 0
	for {
		//passing header
		if i == 0 {
			i++
			continue
		}
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(i, " ", idpar)
			return log, err
		}
		if row[indexId] == idpar {
			found = true
			//fmt.Println(i, " th row")
			for i := 0; i < len(c.Rules.Fields); i++ {
				rowmap[c.Rules.Fields[i]] = row[i]
			}
			//fmt.Println(rowmap)
			//return rowmap, nil
			break
		}
		i++
	}
	if !found {
		log = idpar
	}
	thisRow.Rules = rowmap
	return log, nil
}
