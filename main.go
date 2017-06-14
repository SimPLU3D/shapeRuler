package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	cfg "shapeRuler/config"
	"shapeRuler/rules"
	"shapeRuler/shapeUtils"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/urfave/cli"
)

func writelog(filename string, lines []string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, s := range lines {
		f.WriteString(s + "\n")
	}
	return nil
}

func process(baseDir string, conf *cfg.ConfJson, db *sql.DB, logs *[]string, errors *[]string) {
	parcelFile := baseDir + conf.ParcelShape.Name
	parcellData, err := shapeUtils.ReadInputParcel(parcelFile, conf)
	noMatchingIds := 0
	if err != nil {
		*errors = append(*errors, parcelFile+" : Error reading shapefile "+err.Error())
		return
	}
	log := shapeUtils.DetectSimilarKeys(parcellData)
	if log != "" {
		*logs = append(*logs, "--Multiple same idpar in "+parcelFile+" : "+log)
	}
	log = ""
	for k, _ := range parcellData {
		key, err := rules.PgGetRulesFor(&parcellData[k], conf, db, baseDir)
		if err != nil {
			*errors = append(*errors, parcelFile+" : Error getting rules "+err.Error())
		}
		if key != "" {
			log += key + " "
			noMatchingIds++
			//fmt.Println(log)
			//*logs = append(*logs, log)
		}
	}
	//list of keys not found
	if log != "" {
		norows := ""
		if noMatchingIds == len(parcellData) {
			norows = "!Empty! "
		}
		log = norows + baseDir + " --> missing rules (" +
			strconv.Itoa(noMatchingIds) + "/" + strconv.Itoa(len(parcellData)) + ") for : " + log
		*logs = append(*logs, log)
	}
	err = shapeUtils.WriteShape(baseDir, conf, parcellData)
	if err != nil {
		*errors = append(*errors, parcelFile+" : Error writing shapefile "+err.Error())
		return
	}
}

func processCsv(baseDir string, conf *cfg.ConfJson, logs *[]string, errors *[]string) {
	parcelFile := baseDir + conf.ParcelShape.Name
	parcellData, err := shapeUtils.ReadInputParcel(parcelFile, conf)
	noMatchingIds := 0
	if err != nil {
		*errors = append(*errors, parcelFile+" : Error reading shapefile "+err.Error())
		return
	}
	log := shapeUtils.DetectSimilarKeys(parcellData)
	if log != "" {
		*logs = append(*logs, "--Multiple same idpar in "+parcelFile+" : "+log)
	}
	log = ""
	for k, _ := range parcellData {
		csvFile, err := os.Open(conf.Rules.Access)
		if err != nil {
			*errors = append(*errors, parcelFile+" : Error Opening csvfile "+err.Error())
			return
		}
		defer csvFile.Close()

		key, err := rules.CsvGetRulesFor(&parcellData[k], conf, csvFile, baseDir)
		if err != nil {
			*errors = append(*errors, parcelFile+" : Error getting rules "+err.Error())
		}
		if key != "" {
			log += key + " "
			noMatchingIds++
		}
	}
	//list of keys not found
	if log != "" {
		norows := ""
		if noMatchingIds == len(parcellData) {
			norows = "!Empty! "
		}
		log = norows + baseDir + " --> missing rules (" +
			strconv.Itoa(noMatchingIds) + "/" + strconv.Itoa(len(parcellData)) + ") for : " + log
		*logs = append(*logs, log)
	}
	err = shapeUtils.WriteShape(baseDir, conf, parcellData)
	if err != nil {
		*errors = append(*errors, parcelFile+" : Error writing shapefile "+err.Error())
		return
	}
}

func createCsvForIdparAndDir(baseDir string, conf *cfg.ConfJson, csvslice *[]string) {
	parcelFile := baseDir + conf.ParcelShape.Name
	pathElems := strings.Split(baseDir, "/")
	dir := pathElems[len(pathElems)-2]
	dep := pathElems[len(pathElems)-3][3:]
	parcellData, err := shapeUtils.ReadInputParcel(parcelFile, conf)
	if err != nil {
		fmt.Println(parcelFile + " : Error reading shapefile " + err.Error())
		return
	}
	for _, row := range parcellData {
		*csvslice = append(*csvslice, row.Key+";"+dir+";"+dep)
	}
}

func createStub(c *cli.Context) error {
	if c.String("shape") == "" || c.String("rules") == "" {
		fmt.Println("need options, --help, -h for help")
		return errors.New("bad arguments")
	}
	cfg.CreateStub(shapeFile, rulesFile, csvSep, confOut)
	return nil
}

func execute(c *cli.Context) error {
	if c.String("dir") == "" {
		fmt.Println("need argument for dir, --help, -h for help")
		return errors.New("bad arguments")
	}
	var conf cfg.ConfJson
	var stats []string
	var errs []string
	logfile := "logstats.log"
	errfile := "logerrs.log"
	/* Loading JSON conf file */
	err := conf.LoadFile(confPath)
	if err != nil {
		fmt.Println("error reading conf file " + err.Error())
		panic(err)
	}
	isDb := false
	var db *sql.DB
	if conf.Rules.Type == "db" {
		/* DB object to query the rules*/
		db, err = conf.GetDb() //sql.Open("postgres", dbCreds)
		if err != nil {
			panic(err)
		}
		isDb = true
		defer db.Close()
	}
	/* Loading subdirectories from base path*/
	baseDirPath = sanitizePath(baseDirPath)
	subdirs, err := ioutil.ReadDir(baseDirPath)
	nbDirs := len(subdirs)
	if err != nil {
		panic(err)
	}

	/* Launching the process for all subdirs */
	fmt.Println("Beginning processing for ", nbDirs, " directories")
	start := time.Now()
	for c, d := range subdirs {
		if isDb {
			process(baseDirPath+d.Name()+"/", &conf, db, &stats, &errs)
		} else {
			processCsv(baseDirPath+d.Name()+"/", &conf, &stats, &errs)
		}
		if c > 0 && c%350 == 0 {
			fmt.Println("iter ", c, " - log: ", len(stats), ":", " - errs: ", len(errs), "after ", time.Now().Sub(start).Minutes(), "minutes")
		}
	}
	end := time.Now()
	fmt.Println("Time passed before writing logs (mn) ", (end.Sub(start).Minutes()))
	fmt.Println("log size: ", len(stats))
	fmt.Println("errs size: ", len(errs))

	logPath = sanitizePath(logPath)
	err = writelog(logPath+logfile, stats)
	if err != nil {
		fmt.Println("error writing logfile ", logPath+logfile)
	}
	fmt.Println("log file written in ", logPath+logfile)
	err = writelog(logPath+errfile, errs)
	if err != nil {
		fmt.Println("error writing logfile ", logPath+errfile)
	}
	fmt.Println("log file written in ", logPath+errfile)
	return nil
}

func sanitizePath(dirPath string) string {
	size := len(dirPath)
	if size == 0 {
		return dirPath
	}
	if dirPath[size-1:] != "/" {
		return dirPath + "/"
	}
	return dirPath
}

var confPath, baseDirPath, logPath, rulesFile, shapeFile, csvSep, confOut string

func main() {
	//	dep := "95"
	//	confPath := "/media/imran/Data_2/scripts/confShapesAndRules.json"
	//	baseDir := "/media/imran/Data_2/smallSample/"
	//	baseDir = "/media/imran/Data_2/klodo/" + dep + "/COGIT28112016/dep" + dep + "/"
	//	logdir := "/home/imran/"
	//	//logfile := "logstats_" + dep + ".log"
	//	//errfile := "logerrs_" + dep + ".log"

	//	var conf cfg.ConfJson
	//	var stats []string
	//	var errs []string
	//	var idpar_dir []string

	//	/* Loading JSON conf file */
	//	err := conf.LoadFile(confPath)
	//	if err != nil {
	//		fmt.Println("error reading conf file " + err.Error())
	//		panic(err)
	//	}

	//	/* DB object to query the rules*/
	//	db, err := conf.GetDb() //sql.Open("postgres", dbCreds)
	//	if err != nil {
	//		panic(err)
	//	}
	//	defer db.Close()

	//	/* Loading subdirectories from base path*/
	//	subdirs, err := ioutil.ReadDir(baseDir)
	//	nbDirs := len(subdirs)
	//	if err != nil {
	//		panic(err)
	//	}

	//	/* Launching the process for all subdirs */
	//	fmt.Println("Beginning processing for ", nbDirs, " directories")
	//	start := time.Now()
	//	for c, d := range subdirs {
	//		process(baseDir+d.Name()+"/", &conf, db, &stats, &errs)
	//		//createCsvForIdparAndDir(baseDir+d.Name()+"/", &conf, &idpar_dir)
	//		//processCsv(baseDir+d.Name()+"/", &conf, &stats, &errs)
	//		if c > 0 && c%350 == 0 {
	//			fmt.Println("iter ", c, " - log: ", len(stats), ":", " - errs: ", len(errs), "after ", time.Now().Sub(start).Minutes(), "minutes")
	//		}
	//	}
	//	end := time.Now()

	//	fmt.Println("Time passed before writing logs (mn) ", (end.Sub(start).Minutes()))
	//	fmt.Println("log size: ", len(stats))
	//	fmt.Println("errs size: ", len(errs))
	//	//	err = writelog(logdir+logfile, stats)
	//	//	if err != nil {
	//	//		fmt.Println("error writing logfile ", logdir+logfile)
	//	//	}
	//	//	fmt.Println("log file written in ", logdir+logfile)
	//	//	err = writelog(logdir+errfile, errs)
	//	//	if err != nil {
	//	//		fmt.Println("error writing logfile ", logdir+errfile)
	//	//	}
	//	//	fmt.Println("log file written in ", logdir+errfile)
	//	err = writelog(logdir+"idpars_dir_"+dep+".csv", idpar_dir)
	//	if err != nil {
	//		fmt.Println("error writing logfile ", logdir+"idpars_dir")
	//	}
	//	fmt.Println("csv file written in ", logdir+"idpars_dir_"+dep+".csv")

	//	//cfg.CreateStub(baseDir+"77007018/parcelle.shp", "/media/imran/Data_2/scripts/regles_plu.csv", ";", "polo.json")

	/************** beginning cli parser **************/
	app := cli.NewApp()
	app.EnableBashCompletion = true
	app.Commands = []cli.Command{
		{
			Name:    "createStub",
			Aliases: []string{"cs"},
			Usage:   "create a stub conf file from a typical parcelle shapefile and a rules csv file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "shape, s",
					Usage:       "get the fields from shapefile `FILE.shp`",
					Destination: &shapeFile,
				},
				cli.StringFlag{
					Name:        "rules, r",
					Usage:       "get the headers from csv file `FILE.csv`",
					Destination: &rulesFile,
				},
				cli.StringFlag{
					Name:        "out, o",
					Value:       "generatedConf.json",
					Usage:       "output file name",
					Destination: &confOut,
				},

				cli.StringFlag{
					Name:        "sep",
					Value:       ";",
					Usage:       "separator used in csv",
					Destination: &csvSep,
				},
			},
			Action: createStub,
		},
		{
			Name:    "execute",
			Aliases: []string{"e"},
			Usage:   "Modify the shapefiles according to the JSON configuration file from",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "conf, c",
					Value:       "./conf.json",
					Usage:       "path to the configuration file `FILE.json`",
					Destination: &confPath,
				},
				cli.StringFlag{
					Name:        "dir, d",
					Usage:       "path to subdirectories containing shapefiles",
					Destination: &baseDirPath,
				},
				cli.StringFlag{
					Name:        "log, l",
					Usage:       "path for the generated logfiles",
					Value:       ".",
					Destination: &logPath,
				},
			},
			Action: execute,
		},
	}
	app.Name = "shapeRuler"
	app.Version = "0.70"
	app.Usage = "Tool to add rules in shapefiles, for running a simplu3D simulation"
	app.Run(os.Args)

}
