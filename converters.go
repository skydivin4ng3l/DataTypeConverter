package DataTypeConverter

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	ptypes "github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/natefinch/lumberjack"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

// const LOG_FILE = "/parseError.log"
var errLog *log.Logger

func setupLogFile() {
	e, err := os.OpenFile("./foo.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		fmt.Printf("error opening file: %v", err)
		os.Exit(1)
	}
	errLog = log.New(e, "", log.Ldate|log.Ltime)
	errLog.SetOutput(&lumberjack.Logger{
		Filename:   "./foo.log",
		MaxSize:    1,  // megabytes after which new file is created
		MaxBackups: 3,  // number of backups
		MaxAge:     28, //days
	})
}

func storeFailiure(unparseable string, conFailStat *sync.Map) {
	counter, ok := conFailStat.Load(unparseable)
	if ok {
		conFailStat.Store(unparseable, counter.(int64)+1)
	} else {
		var once int64 = 1
		conFailStat.Store(unparseable, once)
	}
}

func PrintFailStat(conFailStat *sync.Map) {
	setupLogFile()
	conFailStat.Range(func(unparseable, counter interface{}) bool {
		logrus.Infof("Was NOT able to parse: %s  %d times!", unparseable.(string), counter.(int64))
		errLog.Printf("Was NOT able to parse: %s  %d times!", unparseable.(string), counter.(int64))
		return true
	})
}

// this converts the J, B notation of bools to go bools
func ToBool(s string) bool {
	if s == "J" {
		return true
	} else {
		return false
	}
}

//convinience function to cat errors
func ParseStringToFloat64(s string, conFailStat *sync.Map) float64 {
	number, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		storeFailiure("'"+s+"' asFloat64", conFailStat)
		return math.MaxFloat64
	}
	return number
}

func ParseStringToDecimal(s string, conFailStat *sync.Map) decimal.Decimal {
	number, err := decimal.NewFromString(s)
	if err != nil {
		storeFailiure("'"+s+"' asDecimal", conFailStat)
		return decimal.New(math.MinInt64, math.MinInt32)
	}
	return number
}

func ParseStringToInt64(s string, conFailStat *sync.Map) int64 {
	number, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		decimalNumber, err := decimal.NewFromString(s)
		if err != nil {
			storeFailiure("'"+s+"' asInt64", conFailStat)
			return math.MinInt64
		}
		return decimalNumber.IntPart()
	}
	return number
}

//this is copied form the tmpmodels
func ToTimestamp(t time.Time) *tspb.Timestamp {
	ts, _ := ptypes.TimestampProto(t)
	return ts
}

// 01-APR-19 03.12.00.000000000 PM +02:00
// 01-APR-19 03.12.00 PM +02:00
// 01-APR-19 03.12.00.000000000 PM GMT
// 20181231231649+0000 <- YYYYMMDDHHMMSS+0000
// 2019-01-01 00:00:00.0
// 2019-01-01
// 30.12.2018-00:00
func ParseStringToTimestamp(s string, conFailStat *sync.Map) *tspb.Timestamp {

	return ToTimestamp(ParseStringToTime(s, conFailStat))
}

// 01-APR-19 03.12.00.000000000 PM +02:00
// 01-APR-19 03.12.00 PM +02:00
// 01-APR-19 03.12.00.000000000 PM GMT
// 20181231231649+0000 <- YYYYMMDDHHMMSS+0000
// 2019-01-01 00:00:00.0
// 2019-01-01
// 30.12.2018-00:00
func ParseStringToTime(s string, conFailStat *sync.Map) time.Time {
	localtime, _ := time.LoadLocation("Europe/Berlin")
	time.Local = localtime
	importLayouts := []string{
		// for layout information: https://yourbasic.org/golang/format-parse-string-time-date-example/
		"02-Jan-06",
		"02-01-2006",
		"02.01.2006",
		"02.01.2006-03:04",
		"02.01.2006-15:04",
		"2006-01-02",
		"2006-01-02 15:04:05.999",
		"2006-01-02 03:04:05.999",
		"20060102030405-0700",
		"20060102150405-0700", // have to test this	20190609133749+0000
		"02-Jan-06 03.04.05 PM -07:00",
		"02-Jan-06 03.04.05.000000000 PM MST",
		"02-Jan-06 03.04.05.000000000 PM -07:00",
		"20060102",
		"20060102 1504",
		// "200610230405",
		"20060102 150405",
		"20060102 30405",
		"20060102 304",
	}
	for _, importLayout := range importLayouts {
		newTimestamp, err := time.Parse(importLayout, s)
		if err == nil {
			// fmt.Printf("String: %s got parsed to: %v \n", s, newTimestamp)
			return newTimestamp
		}
	}
	storeFailiure("'"+s+"' asTime", conFailStat)
	return time.Time{}
}
