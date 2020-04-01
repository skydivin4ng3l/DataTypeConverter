package DataTypeConverter

import (
	"errors"
	"fmt"
	"log"
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

const LOG_FILE = "./parseError.log"

var errLog *log.Logger

func setupLogFile() {
	e, err := os.OpenFile(LOG_FILE, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		fmt.Printf("error opening file: %v", err)
		os.Exit(1)
	}
	errLog = log.New(e, "", log.Ldate|log.Ltime)
	errLog.SetOutput(&lumberjack.Logger{
		Filename:   LOG_FILE,
		MaxSize:    1,  // megabytes after which new file is created
		MaxBackups: 3,  // number of backups
		MaxAge:     28, //days
	})
}

type Parser interface {
	storeFailiure()
	ParseStringToFloat64() float64
	ParseStringToDecimal() decimal.Decimal
	ParseStringToInt64() int64
	ParseStringToTimestamp() *tspb.Timestamp
	stringRemoveTZOffset(string) (string, error)
}

type LoggedParseString struct {
	s           string
	conFailStat *sync.Map
}

func (ps LoggedParseString) storeFailiure() {
	storeFailiure(ps.s, ps.conFailStat)
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

func (ps LoggedParseString) ParseStringToFloat64() float64 {
	return ParseStringToFloat64(ps.s, ps.conFailStat)
}

//convinience function to cat errors
func ParseStringToFloat64(s string, conFailStat *sync.Map) float64 {
	number, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		storeFailure("'"+s+"' asFloat64", conFailStat)
		return 0.0
	}
	return number
}

func (ps LoggedParseString) ParseStringToDecimal() decimal.Decimal {
	return ParseStringToDecimal(ps.s, ps.conFailStat)
}

func ParseStringToDecimal(s string, conFailStat *sync.Map) decimal.Decimal {
	number, err := decimal.NewFromString(s)
	if err != nil {
		storeFailure("'"+s+"' asDecimal", conFailStat)
		return decimal.NewFromInt(0)
	}
	return number
}

func (ps LoggedParseString) ParseStringToInt64() int64 {
	return ParseStringToInt64(ps.s, ps.conFailStat)
}

func ParseStringToInt64(s string, conFailStat *sync.Map) int64 {
	number, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		decimalNumber, err := decimal.NewFromString(s)
		if err != nil {
			storeFailure("'"+s+"' asInt64", conFailStat)
			return 0
		}
		return decimalNumber.IntPart()
	}
	return number
}

//this is copied form the tmpmodels
func ToTimestamp(t time.Time) *tspb.Timestamp {

	if (t == time.Time{}){
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
func (ps LoggedParseString) ParseStringToTimestamp() *tspb.Timestamp {
	return ParseStringToTimestamp(ps.s, ps.conFailStat)
}

func ParseStringToTimestamp(s string, conFailStat *sync.Map) *tspb.Timestamp {

	return ToTimestamp(ParseStringToTime(s, conFailStat))
}

func (ps LoggedParseString) stringRemoveTZOffset(tz_suffix_layout string) (string, error) {
	return stringRemoveTZOffset(ps.s, ps.conFailStat, tz_suffix_layout)
}

// Cuts tz_suffix = '+/-07:00' || '+/-0700' from prefix+07:00 (7 exemplary)
func stringRemoveTZOffset(s string, conFailStat *sync.Map, tz_suffix_layout string) (string, error) {
	var err error
	s_len := len(s)
	var splitsPlus, splitMinus []string

	splitsPlus = strings.Split(s, "+")
	s_prefix := splitsPlus[0]

	if len(splitsPlus) <= 1 {
		splitMinus = strings.Split(s, "-")
		splitMinus_len := len(splitMinus)
		s_prefix = strings.Join(splitMinus[:splitMinus_len-1], "-")
		if len(splitMinus[splitMinus_len-1]) < len(tz_suffix_layout)-1 {
			err = errors.New("TZ Suffix has not the Format " + tz_suffix_layout)
		}
	} else if len(splitsPlus[1]) < len(tz_suffix_layout)-1 {
		err = errors.New("TZ Suffix has not the Format " + tz_suffix_layout)
	}

	if err != nil || len(s_prefix)+len(tz_suffix_layout) != s_len {
		storeFailiure("'"+s+"' could not remove TimeZone with Format "+tz_suffix_layout, conFailStat)
	}
	s_prefix = strings.TrimSpace(s_prefix)
	return s_prefix, err
}

func ParseStringToDate(s string, conFailStat *sync.Map) *tspb.Timestamp {
	stringTZFree, err := stringRemoveTZOffset(s, conFailStat, "-07:00")
	if err != nil {
		storeFailure("'"+s+"' asDate", conFailStat)
		return nil
	}
	return ToTimestamp(ParseStringToTime(stringTZFree, conFailStat))
}

func TryLayoutsToParseStringToTime(s string, conFailStat *sync.Map, importLayouts []string) (time.Time, error) {
	for _, importLayout := range importLayouts {
		newTime, err := time.Parse(importLayout, s)

		if err == nil {
			// fmt.Printf("String: %s got parsed to: %v \n", s, newTime)
			return newTime, err
		}
	}
	storeFailiure("'"+s+"' asTime", conFailStat)
	return time.Time{}, errors.New("Could not Parse with this ImportLayouts")
}

func (p Pair) removeTZ(s string, conFailStat *sync.Map) (string, error) {
	var err error
	var s_prefix string
	tz_suffix_layout, ok := p[1].(string)
	if ok {
		switch tz_suffix_len := len(tz_suffix_layout); tz_suffix_len {
		case 3 /* GMT */ :
		case 5 /* +/-0700 */, 6 /* +/-07:00 */ :
			s_prefix, err = stringRemoveTZOffset(s, conFailStat, tz_suffix_layout)
		default:
			err = errors.New("Can not Remove TZ")
		}
		//TODO
		return s_prefix, err
	}

	return "", errors.New("TZ Layout needs to be string")
}

func TryLayoutsToParseStringToTimeWithoutTZ(s string, conFailStat *sync.Map, importLayoutsWithoutTimeZone []Pair) (time.Time, error) {
	for _, importLayoutPair := range importLayoutsWithoutTimeZone {
		s_TZFree, tz_err := importLayoutPair.removeTZ(s, conFailStat)
		if tz_err == nil {
			newTime, err := time.Parse(importLayoutPair[0].(string), s_TZFree)
			if err == nil {
				return newTime, err
			}
		}
	}
	storeFailiure("'"+s+"' asTime without TimeZone", conFailStat)
	return time.Time{}, errors.New("Could not Parse without TZ with this ImportLayouts")

	// stringTZFree, tz_err := stringRemoveTZOffset(s, conFailStat)
	// if tz_err != nil {
	// 	//TODO: TZAbr removal
	// }
	// newTime, err := TryLayoutsToParseStringToTime(stringTZFree, importLayoutsWithoutTimeZone[:], conFailStat)
	// if err != nil {
	// 	storeFailiure("'"+s+"' asTime without TimeZone", conFailStat)
	// }

	// return newTime, errors.New("Could not Parse without TZ with this ImportLayouts")
}

type Pair [2]interface{}

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
	importLayoutsWithoutTimeZone := []Pair{
		Pair{"2006-01-02", "-07:00"}, //TODO Needs Rework: Here should be the Layouts after the removal of TZ
	}

	newTime := time.Time{}

	newTime, err := TryLayoutsToParseStringToTime(s, conFailStat, importLayouts[:])
	if err != nil {
		newTime, err = TryLayoutsToParseStringToTimeWithoutTZ(s, conFailStat, importLayoutsWithoutTimeZone[:])
		if err != nil {
			storeFailiure("'"+s+"' asTime", conFailStat)
		}
	}

	return newTime
	/* for _, importLayout := range importLayouts {
		newTimestamp, err := time.Parse(importLayout, s)

		if err == nil {
			// fmt.Printf("String: %s got parsed to: %v \n", s, newTimestamp)
			return newTimestamp
		}
	}
	storeFailiure("'"+s+"' asTime", conFailStat)
	return time.Time{} */

}
