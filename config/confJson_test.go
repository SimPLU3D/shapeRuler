package config

import (
	//"fmt"
	"bytes"
	"testing"
)

func TestLoadFile(t *testing.T) {
	var conf ConfJson
	confFile := "../resources/confShapesAndRules.json"
	conf.LoadFile(confFile)
	if len(conf.ParcelShape.Fields) != 5 ||
		conf.ParcelShape.Name != "parcelle.shp" ||
		conf.ParcelShape.Key != "IDPAR" /*etc.. */ {
		t.Fail()
	} else {
		t.Log("shapefile fields ok")
	}
}

//convert a slices of byte to a string eliminating the pending \0
func b2s(b []byte) string {
	n := bytes.IndexByte(b, 0)
	return string(b[:n])
}

func TestCreateShapeFields(t *testing.T) {
	var conf ConfJson
	nbFields := 56
	confFile := "../resources/confShapesAndRules.json"
	conf.LoadFile(confFile)
	fieldsOut := conf.CreateShapeFields()
	if len(fieldsOut) != nbFields ||
		b2s(fieldsOut[0].Name[:]) != "IDPAR" || fieldsOut[0].Fieldtype != 'C' || fieldsOut[0].Size != 50 ||
		b2s(fieldsOut[1].Name[:]) != "ID_Parcell" || fieldsOut[1].Fieldtype != 'N' || fieldsOut[1].Size != 9 ||
		b2s(fieldsOut[3].Name[:]) != "Shape_Leng" || fieldsOut[3].Fieldtype != 'F' || fieldsOut[3].Size != 19 {
		t.Fail()
		t.Log("fields type does not match configuration")
	} else {
		t.Log("OK")
	}
}
