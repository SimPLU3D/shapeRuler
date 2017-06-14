package config

import (
	//"fmt"
	"testing"
)

func TestLoadFile(t *testing.T) {
	var conf ConfJson
	confFile := "../resources/confShapesAndRules.json"
	conf.LoadFile(confFile)
	if conf.ParcelShape.Name != "parcelle.shp" ||
		conf.ParcelShape.Key != "IDPAR" ||
		len(conf.ParcelShape.Fields) != 5 {
		t.Fail()
	} else {
		t.Log("shapefile fields ok")
	}
}
