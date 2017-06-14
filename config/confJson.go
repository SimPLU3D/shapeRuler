package config

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/jonas-p/go-shp"
)

type Correspondance struct {
	Corresponding string `json:"corresponding"`
	From          string `json:"from"`
	Name          string `json:"name"`
	Size          int    `json:"size"`
	Type          string `json:"type"`
}
type ConfJson struct {
	ParcelShape struct {
		Fields []string `json:"fields"`
		Key    string   `json:"key"`
		Name   string   `json:"name"`
	} `json:"parcelShape"`
	Rules struct {
		Fields []string `json:"fields"`
		Access string   `json:"access"`
		Key    string   `json:"key"`
		Type   string   `json:"type"`
		Table  string   `json:"table"`
	} `json:"rules"`
	ShapeOut struct {
		Fields []Correspondance `json:"fields"`
		Name   string           `json:"name"`
	} `json:"shapeOut"`
}

func (c *ConfJson) CreateShapeFields() []shp.Field {
	var fields []shp.Field
	for _, data := range c.ShapeOut.Fields {
		switch {
		case data.Type == "String":
			fields = append(fields, shp.StringField(data.Name, uint8(data.Size)))
		case data.Type == "Number":
			fields = append(fields, shp.NumberField(data.Name, uint8(data.Size)))
		case data.Type == "Float":
			fields = append(fields, shp.FloatField(data.Name, uint8(data.Size), 11))
		}
	}
	return fields
}

func (c *ConfJson) LoadFile(confPath string) error {
	f, err := ioutil.ReadFile(confPath)
	if err != nil {
		fmt.Println("error reading conf file")
		return err
	}
	err = json.Unmarshal(f, &c)
	if err != nil {
		fmt.Println("error unmarshalling conf file")
		return err
	}
	return nil
}

type DBcreds struct {
	DbName   string `json:"dbName"`
	Password string `json:"password"`
	Path     string `json:"path"`
	Type     string `json:"type"`
	User     string `json:"user"`
}

func (c *ConfJson) GetDbURI() (string, error) {
	var creds DBcreds
	f, err := ioutil.ReadFile(c.Rules.Access)
	if err != nil {
		fmt.Println("error reading db credentials file")
		return "", err
	}
	err = json.Unmarshal(f, &creds)
	if err != nil {
		fmt.Println("error unmarshalling db creds file")
		return "", err
	}
	//"postgres://imrandb:imrandb@localhost/iudf"
	uri := creds.Type + "://" + creds.User + ":" + creds.Password + "@" + creds.Path + "/" + creds.DbName
	return uri, nil
}

func (c *ConfJson) GetDb() (*sql.DB, error) {
	if c.Rules.Type != "db" {
		return nil, errors.New("rules is not taken from db in conf file")
	}
	var creds DBcreds
	f, err := ioutil.ReadFile(c.Rules.Access)
	if err != nil {
		fmt.Println("error reading db credentials file")
		return nil, err
	}
	err = json.Unmarshal(f, &creds)
	if err != nil {
		fmt.Println("error unmarshalling db creds file")
		return nil, err
	}
	//"postgres://imrandb:imrandb@localhost/iudf"
	switch creds.Type {
	case "sqlite":
		return sql.Open("sqlite3", creds.Path)
	case "postgres":
		uri := creds.Type + "://" + creds.User + ":" + creds.Password + "@" + creds.Path + "/" + creds.DbName
		return sql.Open("postgres", uri)
	default:
		return nil, errors.New("DB type unknown in db creds conf file(only postgres or sqlite3 currently supported)")
	}
}

func getShpHeaders(fileName string) []string {
	shape, err := shp.Open(fileName)
	if err != nil {
		fmt.Println("error reading parcelle file ", fileName, " ", err.Error())
		fmt.Println(err)
		return nil
	}
	defer shape.Close()
	var fields []string
	for _, f := range shape.Fields() { //f is [11]byte whe need to make it a stringf.Name
		sb := f.Name[:]
		n := bytes.IndexByte(sb, 0)
		fields = append(fields, string(sb[:n]))
	}
	return fields
}

func getCsvHeader(reglesFile string, sep string) []string {
	var fields []string
	f, err := os.Open(reglesFile)
	if err != nil {
		fmt.Println("could not read file " + err.Error())
		return nil
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		head := scan.Text()
		fields = strings.Split(head, sep)
		break
	}
	return fields
}

func CreateStub(parcelleFile string, reglesFile string, sep string, outjson string) {
	confStub := ConfJson{}
	fields := getShpHeaders(parcelleFile)
	// parcelle
	confStub.ParcelShape.Name = "parcelle.shp"
	confStub.ParcelShape.Key = "IDPAR"
	confStub.ParcelShape.Fields = fields
	// regles
	fields = getCsvHeader(reglesFile, sep)
	confStub.Rules.Type = "db"
	confStub.Rules.Access = "/path/to/creds.json"
	confStub.Rules.Key = "idpar"
	confStub.Rules.Table = "regles_plu"
	confStub.Rules.Fields = fields
	//shapeOut
	confStub.ShapeOut.Name = "parcelle_new.shp"
	confStub.ShapeOut.Fields = []Correspondance{
		Correspondance{Name: "IDPAR", Type: "String", Size: 50, From: "parcelShape", Corresponding: "IDPAR"},
		Correspondance{Name: "ID_Parcell", Type: "Number", Size: 9, From: "parcelShape", Corresponding: "ID_Parcell"},
		Correspondance{Name: "Shape_Area", Type: "Float", Size: 19, From: "parcelShape", Corresponding: "Shape_Area"},
		Correspondance{Name: "IMU", Type: "String", Size: 80, From: "rules", Corresponding: "code_imu"},
		Correspondance{Name: "ZONAGE_COH", Type: "Number", Size: 10, From: "new", Corresponding: "1"},
	}

	jsonFile, err := os.Create(outjson)
	if err != nil {
		fmt.Println("Error creating JSON file:", err)
		return
	}
	jsonWriter := io.Writer(jsonFile)
	encoder := json.NewEncoder(jsonWriter)
	err = encoder.Encode(&confStub)
	if err != nil {
		fmt.Println("Error encoding JSON to file:", err)
		return
	}
	fmt.Println("wrote " + outjson)
}
