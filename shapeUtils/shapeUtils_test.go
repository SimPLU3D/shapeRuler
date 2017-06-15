package shapeUtils

import (
	"shapeRuler/config"
	"testing"
)

func TestReadInputParcel(t *testing.T) {
	nbRows := 7
	parcelFile := "../resources/77006871/parcelle.shp"
	confFile := "../resources/confShapesAndRules.json"
	var conf config.ConfJson
	conf.LoadFile(confFile)
	parcellData, err := ReadInputParcel(parcelFile, &conf)
	if err != nil {
		t.Fail()
		t.Log(err.Error())
	}

	//very basic
	if len(parcellData) != nbRows {
		t.Fail()
		t.Log("numbers of rows loaded != number of rows in shapefile")
	} else {
		t.Log("Parcel shapefile coorectly loaded")
	}
}
