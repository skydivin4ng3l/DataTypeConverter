package logger

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/natefinch/lumberjack"
	"github.com/sirupsen/logrus"
)

//LogFile The log filename which gets created in the executing directory
const LogFile = "./parseError.log"

var errLog *log.Logger

//SetupLogFile creates a tummeling log file named after const LogFile
func SetupLogFile() {
	e, err := os.OpenFile(LogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		fmt.Printf("error opening file: %v", err)
		os.Exit(1)
	}
	errLog = log.New(e, "", log.Ldate|log.Ltime)
	errLog.SetOutput(&lumberjack.Logger{
		Filename:   LogFile,
		MaxSize:    1,  // megabytes after which new file is created
		MaxBackups: 3,  // number of backups
		MaxAge:     28, //days
	})
}

//StoreFailure stores a string s into a map which counts how many times s getts added
func StoreFailure(unparseable string, conFailStat *sync.Map) {
	counter, ok := conFailStat.Load(unparseable)
	if ok {
		conFailStat.Store(unparseable, counter.(int64)+1)
	} else {
		var once int64 = 1
		conFailStat.Store(unparseable, once)
	}
}

// PrintFailStat prints Statistics of Failed LoggedParseString parsing to console and the LogFile
func PrintFailStat(conFailStat *sync.Map) {
	SetupLogFile()
	conFailStat.Range(func(unparseable, counter interface{}) bool {
		logrus.Infof("Was NOT able to parse: %s  %d times!", unparseable.(string), counter.(int64))
		errLog.Printf("Was NOT able to parse: %s  %d times!", unparseable.(string), counter.(int64))
		return true
	})
	errLog.Printf("-----> End of logging of last Collection and beginning of new Collection if any")
}
