package shapeUtils

import (
	//"bytes"
	"fmt"
	cfg "shapeRuler/config"

	"strconv"

	"github.com/jonas-p/go-shp"
)

/*
basic structure to store a row for the output shape
*/
type ShapeRow struct {
	Key    string
	Values map[string]string
	Geom   shp.Shape
	Rules  map[string]string
}

/*
read and store the input parcel data from path "file" in a slice of shapeRow struct
needs a conf file for corresponding fields names
*/
func ReadInputParcel(file string, c *cfg.ConfJson) ([]ShapeRow, error) {
	shape, err := shp.Open(file)
	if err != nil {
		fmt.Println("error reading parcelle file ", file, " ", err.Error())
		return nil, err
	}
	defer shape.Close()
	var vals []ShapeRow
	fields := shape.Fields()
	for shape.Next() {
		n, s := shape.Shape()
		r := make(map[string]string)
		for k, _ := range fields {
			val := shape.ReadAttribute(n, k)
			nom := c.ParcelShape.Fields[k]
			r[nom] = val
		}
		row := ShapeRow{r[c.ParcelShape.Key], r, s, nil}
		vals = append(vals, row)
	}
	return vals, nil
}

/* Write Shapefile corresponding to a slice of ShapeRow */
func WriteShape(baseDir string, c *cfg.ConfJson, shapeRows []ShapeRow) error {
	filename := baseDir + c.ShapeOut.Name
	shapeW, err := shp.Create(filename, shp.POLYGON)
	defer shapeW.Close()
	if err != nil {
		fmt.Println("error creating shapefile ", filename)
		return err
	}
	fields := c.CreateShapeFields()
	shapeW.SetFields(fields)
	for i, row := range shapeRows {
		shapeW.Write(row.Geom)
		for indexField, f := range c.ShapeOut.Fields {
			var val string
			switch {
			case f.From == "rules":
				val = row.Rules[f.Corresponding]
			case f.From == "parcelShape":
				val = row.Values[f.Corresponding]
			case f.From == "new":
				val = f.Corresponding
			}
			if val == "" {
				continue
			}
			switch {
			case f.Type == "Number":
				//num, err := strconv.Atoi(val)
				num, err := strconv.ParseFloat(val, 64) // for non fomatted csv
				if err != nil {
					fmt.Println("could not write field "+f.Name+" : ", err)
					continue
				}
				shapeW.WriteAttribute(i, indexField, int(num))
			case f.Type == "Float":
				num, err := strconv.ParseFloat(val, 64)
				if err != nil {
					fmt.Println("could not write field "+f.Name+" : ", err)
					continue
				}
				shapeW.WriteAttribute(i, indexField, num)
			default:
				shapeW.WriteAttribute(i, indexField, val)
			}
		}
	}
	return nil
}

/*
 return a string containing thenon unique keys in the shape
*/
func DetectSimilarKeys(rows []ShapeRow) string {
	var log string
	if len(rows) <= 1 {
		return log
	}
	keysNB := make(map[string]int)
	for _, row := range rows {
		keysNB[row.Key] += 1
	}
	for k, e := range keysNB {
		if e > 1 {
			log += k + " "
		}
	}
	return log
}
