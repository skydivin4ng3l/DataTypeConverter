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
	stringRemoveTZAbrevation() (string, error)
	ParseStringToDate() *tspb.Timestamp
	TryLayoutsToParseStringToTime() (time.Time, error)
	removeTZ(Pair) (string, error)
}

type LoggedParseString struct {
	s           string
	conFailStat *sync.Map
}

func (ps LoggedParseString) storeFailure() {
	storeFailure(ps.s, ps.conFailStat)
}

func storeFailure(unparseable string, conFailStat *sync.Map) {
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
func (ps LoggedParseString) ParseStringToTimestamp() *tspb.Timestamp {
	return ParseStringToTimestamp(ps.s, ps.conFailStat)
}

func ParseStringToTimestamp(s string, conFailStat *sync.Map) *tspb.Timestamp {

	return ToTimestamp(ParseStringToTime(s, conFailStat))
}

func (lps LoggedParseString) stringRemoveTZAbrevation() (string, error) {
	var err error
	substrings := strings.Split(lps.s, " ")
	substings_len := len(substrings)
	tz_suffix := substrings[substings_len-1]
	_, err = time.LoadLocation(tz_suffix)
	if err != nil {
		storeFailure("'"+lps.s+"' could not remove "+tz_suffix+" as TimeZone Abbreviation GMT", lps.conFailStat)
		return "", err
	}
	s_prefix := strings.Join(substrings[:substings_len-1], " ")
	s_prefix = strings.TrimSpace(s_prefix)
	return s_prefix, err
}

func (ps LoggedParseString) stringRemoveTZOffset(tz_suffix_layout string) (string, error) {
	return stringRemoveTZOffset(ps, tz_suffix_layout)
}

// Cuts tz_suffix = '+/-07:00' || '+/-0700' || +/-07 from prefix+07:00 (7 exemplary)
func stringRemoveTZOffset(ps LoggedParseString, tz_suffix_layout string) (string, error) {
	var err error
	s_len := len(ps.s)
	var splitsPlus, splitMinus []string

	splitsPlus = strings.Split(ps.s, "+")
	s_prefix := splitsPlus[0]

	if len(splitsPlus) <= 1 {
		splitMinus = strings.Split(ps.s, "-")
		splitMinus_len := len(splitMinus)
		s_prefix = strings.Join(splitMinus[:splitMinus_len-1], "-")
		if len(splitMinus[splitMinus_len-1]) < len(tz_suffix_layout)-1 {
			err = errors.New("TZ Suffix has not the Format " + tz_suffix_layout)
		}
	} else if len(splitsPlus[1]) < len(tz_suffix_layout)-1 {
		err = errors.New("TZ Suffix has not the Format " + tz_suffix_layout)
	}

	if err != nil || len(s_prefix)+len(tz_suffix_layout) != s_len {
		storeFailure("'"+ps.s+"' could not remove TimeZone with Format "+tz_suffix_layout, ps.conFailStat)
	}
	s_prefix = strings.TrimSpace(s_prefix)
	return s_prefix, err
}

func (ps LoggedParseString) ParseStringToDate() *tspb.Timestamp {
	return ParseStringToDate(ps.s, ps.conFailStat)
}

func ParseStringToDate(s string, conFailStat *sync.Map) *tspb.Timestamp {
	stringTZFree, err := stringRemoveTZOffset(LoggedParseString{s, conFailStat}, "-07:00")
	if err != nil {
		storeFailure("'"+s+"' asDate", conFailStat)
		return nil
	}
	return ToTimestamp(ParseStringToTime(stringTZFree, conFailStat))
}

func (lps LoggedParseString) removeTZ(p Pair) (string, error) {
	var err error
	var s_prefix string
	tz_suffix_layout, ok := p[1].(string)
	if ok {
		switch tz_suffix_layout {
		case "GMT":
			s_prefix, err = lps.stringRemoveTZAbrevation()
		case "-0700", "-07:00", "-07":
			s_prefix, err = lps.stringRemoveTZOffset(tz_suffix_layout)
		default:
			err = errors.New("Can not Remove TZ")
		}
		//TODO
		return s_prefix, err
	}

	return "", errors.New("TZ Layout needs to be string")
}

func TryLayoutsToParseStringToTimeWithoutTZ(lps LoggedParseString, importLayoutsWithoutTimeZone []Pair) (time.Time, error) {
	for _, importLayoutPair := range importLayoutsWithoutTimeZone {
		s_TZFree, tz_err := lps.removeTZ(importLayoutPair)
		if tz_err == nil {
			newTime, err := time.Parse(importLayoutPair[0].(string), s_TZFree)
			if err == nil {
				return newTime, err
			}
		}
	}
	storeFailure("'"+lps.s+"' asTime without TimeZone", lps.conFailStat)
	return time.Time{}, errors.New("Could not Parse without TZ with this ImportLayouts")
}

func TryLayoutsToParseStringToTime(lps LoggedParseString, importLayouts []string) (time.Time, error) {
	for _, importLayout := range importLayouts {
		newTime, err := time.Parse(importLayout, lps.s)

		if err == nil {
			// fmt.Printf("String: %s got parsed to: %v \n", s, newTime)
			return newTime, err
		}
	}
	storeFailure("'"+lps.s+"' asTime", lps.conFailStat)
	return time.Time{}, errors.New("Could not Parse with this ImportLayouts")
}

func (lps LoggedParseString) TryLayoutsToParseStringToTime(i interface{}) (time.Time, error) {
	var newTime time.Time
	var err error
	switch v := i.(type) {
	case []string:
		newTime, err = TryLayoutsToParseStringToTime(lps, v)
		return newTime, err
	case []Pair:
		newTime, err = TryLayoutsToParseStringToTimeWithoutTZ(lps, v)
		return newTime, err
	default:
		err = errors.New("Needs []string or []Pair as Parameter")
	}
	return time.Time{}, err
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
		Pair{"2006-01-02", "-07:00"},
		Pair{"15:04:05", "-07:00"},
	}

	newTime := time.Time{}
	loggedParseString := LoggedParseString{s, conFailStat}
	var err error
	newTime, err = loggedParseString.TryLayoutsToParseStringToTime(importLayouts[:])
	if err != nil {
		newTime, err = loggedParseString.TryLayoutsToParseStringToTime(importLayoutsWithoutTimeZone[:])
		if err != nil {
			storeFailure("'"+s+"' asTime", conFailStat)
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
