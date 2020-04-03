package datatypeconverter

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
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
	e, err := os.OpenFile("./parseError.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		fmt.Printf("error opening file: %v", err)
		os.Exit(1)
	}
	errLog = log.New(e, "", log.Ldate|log.Ltime)
	errLog.SetOutput(&lumberjack.Logger{
		Filename:   "./parseError.log",
		MaxSize:    1,  // megabytes after which new file is created
		MaxBackups: 3,  // number of backups
		MaxAge:     28, //days
	})
}

func StoreFailure(unparseable string, conFailStat *sync.Map) {
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
		logrus.Infof("Was NOT able to parse: %s %d times!", unparseable.(string), counter.(int64))
		errLog.Printf("Was NOT able to parse: %s %d times!", unparseable.(string), counter.(int64))
		return true
	})
	errLog.Printf("-----> End of logging of last Collection and beginning of new Collection if any")
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
		StoreFailure("'"+s+"' asFloat64", conFailStat)
		return 0.0
	}
	return number
}

func ParseStringToDecimal(s string, conFailStat *sync.Map) decimal.Decimal {
	number, err := decimal.NewFromString(s)
	if err != nil {
		StoreFailure("'"+s+"' asDecimal", conFailStat)
		return decimal.NewFromInt(0)
	}
	return number
}

// CheckForError checks the given error and stores a possible failure
func CheckForError(err error, rawValue interface{}, t reflect.Kind, failStat *sync.Map, fields ...string) {
	if err != nil {
		var field string
		if len(fields) > 0 {
			field = fmt.Sprintf(`Field %s: `, fields[0])
		}
		StoreFailure(fmt.Sprintf(`%sFailed to parse "%s" as %s: %s`, field, rawValue, t.String(), err.Error()), failStat)
	}
}

// ParseStringToInt64 parses a string to an int64 and stores any failure
func ParseStringToInt64(s string, failStat *sync.Map, fields ...string) int64 {
	number, err := strconv.ParseInt(strings.Replace(s, " ", "", -1), 10, 64)
	if err != nil {
		decimalNumber, err := decimal.NewFromString(s)
		if err != nil {
			CheckForError(err, decimalNumber, reflect.Int64, failStat, fields...)
			return 0
		}
		return decimalNumber.IntPart()
	}
	return number
}

//this is copied form the tmpmodels
func ToTimestamp(t time.Time) *tspb.Timestamp {

	if (t == time.Time{}) {
		return nil
	}

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

// Cuts '+07:00' from prefix+07:00 (7 exemplary)
func stringRemoveTZOffset(s string, conFailStat *sync.Map) (string, error) {
	var err error
	var splitsPlus, splitMinus []string

	splitsPlus = strings.Split(s, "+")
	s_prefix := splitsPlus[0]

	if len(splitsPlus) <= 1 {
		splitMinus = strings.Split(s, "-")
		s_prefix = strings.Join(splitMinus[:3], "-")
		if len(splitMinus[3]) < 5 {
			err = errors.New("TZ Suffix has not the Format -00:00")
		}
	} else if len(splitsPlus[1]) < 5 {
		err = errors.New("TZ Suffix has not the Format +00:00")
	}

	if err != nil {
		StoreFailure("'"+s+"' could not remove TimeZone with Format -/+00:00", conFailStat)
	}

	return s_prefix, err
}

func ParseStringToDate(s string, conFailStat *sync.Map) *tspb.Timestamp {
	stringTZFree, err := stringRemoveTZOffset(s, conFailStat)
	if err != nil {
		StoreFailure("'"+s+"' asDate", conFailStat)
		return nil
	}
	return ToTimestamp(ParseStringToTime(stringTZFree, conFailStat))
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
		"2006-01-02-07:00",
		"2006-01-02T15:04:05.999999-07:00",
		"2006-01-02 15:04:05.999",
		"2006-01-02 03:04:05.999",
		"2006-01-02 03:04:05.999-0700",
		"2006-01-02 15:04:05 -0700",
		"20060102030405-0700",
		"20060102150405-0700", // have to test this	20190609133749+0000
		"20060102150405",
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
	StoreFailure("'"+s+"' asTime", conFailStat)
	return time.Time{}
}
