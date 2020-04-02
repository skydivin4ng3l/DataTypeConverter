package datatypeconverter

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

//Location the Location to Parse the Time in
var Location = "Europe/Berlin"

// TimeLayoutTrimTimeZone defines the tz format tZoneLayoutToRemove
// which shall be removed from a string so that the resulting string has the format tLayoutPostTZRemoval
// so that string can be parsed as time
// example TimeLayoutTrimedTimeZone{"2006-01-02", "-07:00"}
type TimeLayoutTrimTimeZone struct {
	tLayoutPostTZRemoval string
	tZoneLayoutToRemove  string
}

var importLayoutsWithoutTimeZone = []TimeLayoutTrimTimeZone{
	TimeLayoutTrimTimeZone{"2006-01-02", "-07:00"},
	TimeLayoutTrimTimeZone{"15:04:05", "-07:00"},
}

var importLayouts = []string{
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
	"2006-01-02 15:04:05 -07:00",
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

//LogFile The log filename which gets created in the executing directory
const LogFile = "./parseError.log"

var errLog *log.Logger

func setupLogFile() {
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

//Parser offers interface to parse strings currently only LoggedParseString
type Parser interface {
	storeFailiure()
	ParseStringToFloat64() float64
	ParseStringToDecimal() decimal.Decimal
	ParseStringToInt64() int64
	ParseStringToTimestamp() *tspb.Timestamp
	stringSplitTZOffset(string) (string, error)
	stringSplitTZAbbreviation() (string, error)
	ParseStringToDate() *tspb.Timestamp
	TryLayoutsToParseStringToTime() (time.Time, error)
	splitTZ(TimeLayoutTrimTimeZone) (string, string, error)
}

//LoggedParseString a pair of string and a *sync.Map which stores parse erros
type LoggedParseString struct {
	s           string
	conFailStat *sync.Map
}

func (lps LoggedParseString) storeFailure() {
	storeFailure(lps.s, lps.conFailStat)
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

// PrintFailStat prints Statistics of Failed LoggedParseString parsing to console and the LogFile
func PrintFailStat(conFailStat *sync.Map) {
	setupLogFile()
	conFailStat.Range(func(unparseable, counter interface{}) bool {
		logrus.Infof("Was NOT able to parse: %s  %d times!", unparseable.(string), counter.(int64))
		errLog.Printf("Was NOT able to parse: %s  %d times!", unparseable.(string), counter.(int64))
		return true
	})
	errLog.Printf("-----> End of logging of last Collection and beginning of new Collection if any")
}

// ToBool converts the J, B notation of bools to go bools
func ToBool(s string) bool {
	if s == "J" {
		return true
	}
	return false
}

// ParseStringToFloat64 prases the string in LoggedParseString as float64 and logs any failures
func (lps LoggedParseString) ParseStringToFloat64() float64 {
	return ParseStringToFloat64(lps.s, lps.conFailStat)
}

// ParseStringToFloat64 prases the string s as float64 and logs any failures in conFailStat
func ParseStringToFloat64(s string, conFailStat *sync.Map) float64 {
	number, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		storeFailure("'"+s+"' asFloat64", conFailStat)
		return 0.0
	}
	return number
}

// ParseStringToDecimal prases the string in LoggedParseString as decimal.Decimal and logs any failures
func (lps LoggedParseString) ParseStringToDecimal() decimal.Decimal {
	return ParseStringToDecimal(lps.s, lps.conFailStat)
}

// ParseStringToDecimal prases the string s as decimal.Decimal and logs any failures in conFailStat
func ParseStringToDecimal(s string, conFailStat *sync.Map) decimal.Decimal {
	number, err := decimal.NewFromString(s)
	if err != nil {
		storeFailure("'"+s+"' asDecimal", conFailStat)
		return decimal.NewFromInt(0)
	}
	return number
}

// ParseStringToInt64 prases the string in LoggedParseString as int64 and logs any failures
func (lps LoggedParseString) ParseStringToInt64() int64 {
	return ParseStringToInt64(lps.s, lps.conFailStat)
}

// ParseStringToInt64 prases the string s as int64 and logs any failures in conFailStat
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

//ToTimestamp this is copied form the tmpmodels
func ToTimestamp(t time.Time) *tspb.Timestamp {

	if (t == time.Time{}) {
		return nil
	}

	ts, _ := ptypes.TimestampProto(t)
	return ts
}

// ParseStringToTimestamp prases the string in LoggedParseString as *tspb.Timestamp and logs any failures
func (lps LoggedParseString) ParseStringToTimestamp() *tspb.Timestamp {
	return ParseStringToTimestamp(lps.s, lps.conFailStat)
}

// ParseStringToTimestamp prases the string in LoggedParseString as *tspb.Timestamp and logs any failures
func ParseStringToTimestamp(s string, conFailStat *sync.Map) *tspb.Timestamp {

	return ToTimestamp(ParseStringToTime(s, conFailStat))
}

// stringSplitTZAbbreviation removes Timezone Abbreviations seperated by " " like GMT from the string in LoggedParseString and logs any failures
// returns erro if there is no Timezone defined
func (lps LoggedParseString) stringSplitTZAbbreviation() (string, string, error) {
	var err error
	lps.s = strings.TrimSpace(lps.s)
	substrings := strings.Split(lps.s, " ")
	substingsLen := len(substrings)
	tzSuffix := substrings[substingsLen-1]
	_, err = time.LoadLocation(tzSuffix)
	if err != nil {
		storeFailure("'"+lps.s+"' could not remove "+tzSuffix+" as TimeZone Abbreviation GMT", lps.conFailStat)
		return "", "", err
	}
	sPrefix := strings.Join(substrings[:substingsLen-1], " ")
	sPrefix = strings.TrimSpace(sPrefix)
	return sPrefix, tzSuffix, err
}

// stringSplitTZOffset removes Timezone Offsets like -07 -0700 -07:00 from the string in LoggedParseString and logs any failures
// Cuts tz_suffix = '+/-07:00' || '+/-0700' || +/-07 from prefix+07:00 (7 exemplary)
// returns erro if the cut part is short or longer than the tzSuffixLayout defined
func (lps LoggedParseString) stringSplitTZOffset(tzSuffixLayout string) (string, string, error) {
	return stringSplitTZOffset(lps, tzSuffixLayout)
}

// stringSplitTZOffset removes Timezone Offsets like -07 -0700 -07:00 from the string s and logs any failures in conFailStat
// Cuts tz_suffix = '+/-07:00' || '+/-0700' || +/-07 from prefix+07:00 (7 exemplary)
// returns erro if the cut part is short or longer than the tzSuffixLayout defined
func stringSplitTZOffset(lps LoggedParseString, tzSuffixLayout string) (string, string, error) {
	var err error
	s := strings.TrimSpace(lps.s)
	sLength := len(s)
	var splitsPlus, splitsMinus []string

	splitsPlus = strings.Split(s, "+")
	sPrefix := splitsPlus[0]
	tzSuffix := "+" + splitsPlus[1]

	if len(splitsPlus) <= 1 {
		splitsMinus = strings.Split(s, "-")
		splitMinusLength := len(splitsMinus)
		sPrefix = strings.Join(splitsMinus[:splitMinusLength-1], "-")
		if len(splitsMinus[splitMinusLength-1]) < len(tzSuffixLayout)-1 {
			err = errors.New("TZ Suffix has not the Format " + tzSuffixLayout)
		}
		tzSuffix = "-" + splitsMinus[splitMinusLength-1]
	} else if len(splitsPlus[1]) < len(tzSuffixLayout)-1 {
		err = errors.New("TZ Suffix has not the Format " + tzSuffixLayout)
	}

	if err != nil || len(sPrefix)+len(tzSuffixLayout) != sLength {
		storeFailure("'"+lps.s+"' could not remove TimeZone with Format "+tzSuffixLayout, lps.conFailStat)
	}
	sPrefix = strings.TrimSpace(sPrefix)
	return sPrefix, tzSuffix, err
}

//ParseStringToDate !Depricated! removes the timezone offset before parsing the string from LoggedParseString as *tspb.Timestamp and logging any failures
func (lps LoggedParseString) ParseStringToDate() *tspb.Timestamp {
	return ParseStringToDate(lps.s, lps.conFailStat)
}

//ParseStringToDate !Depricated! removes the timezone offset before parsing the string s as *tspb.Timestamp and logging any failures to conFailStat
func ParseStringToDate(s string, conFailStat *sync.Map) *tspb.Timestamp {
	stringTZFree, _, err := stringSplitTZOffset(LoggedParseString{s, conFailStat}, "-07:00")
	if err != nil {
		storeFailure("'"+s+"' asDate", conFailStat)
		return nil
	}
	return ToTimestamp(ParseStringToTime(stringTZFree, conFailStat))
}

func (lps LoggedParseString) splitTZ(p TimeLayoutTrimTimeZone) (string, string, error) {
	var err error
	var sPrefix, tzSuffix string
	tzSuffixLayout := p.tZoneLayoutToRemove

	switch tzSuffixLayout {
	case "GMT":
		sPrefix, tzSuffix, err = lps.stringSplitTZAbbreviation()
	case "-0700", "-07:00", "-07":
		sPrefix, tzSuffix, err = lps.stringSplitTZOffset(tzSuffixLayout)
	default:
		err = errors.New("Can not Remove TZ")
		sPrefix = ""
	}
	return sPrefix, tzSuffix, err
}

//TryLayoutsToParseStringToTimeWithoutTZ trys to parse the string within LoggedParseString to time.Time without Timezone information using the importLayoutsWithoutTimeZone []TimeZoneLayoutToRemove
// example: []TimeZoneLayoutToRemove{TimeZoneLayoutToRemove{"2006-01-02", "-07:00"},	TimeZoneLayoutToRemove{"15:04:05", "-07:00"},}
func TryLayoutsToParseStringToTimeWithoutTZ(lps LoggedParseString, importLayoutsWithoutTimeZone []TimeLayoutTrimTimeZone) (time.Time, error) {
	localtime, _ := time.LoadLocation(Location)
	time.Local = localtime
	for _, importLayoutPair := range importLayoutsWithoutTimeZone {
		stringTZFree, _, tzError := lps.splitTZ(importLayoutPair)
		if tzError == nil {
			// newTime, err := time.Parse(importLayoutPair.tLayoutPostTZRemoval, stringTZFree)
			newTime, err := time.ParseInLocation(importLayoutPair.tLayoutPostTZRemoval, stringTZFree, localtime)
			if err == nil {
				return newTime, err
			}
		}
	}
	storeFailure("'"+lps.s+"' asTime without TimeZone", lps.conFailStat)
	return time.Time{}, errors.New("Could not Parse without TZ with this ImportLayouts")
}

//TryLayoutsToParseStringToTimeWithTZ trys to parse the string within LoggedParseString to time.Time using the importLayouts []string
// for layout information: https://yourbasic.org/golang/format-parse-string-time-date-example/
// example importLayouts := []string{"02.01.2006-03:04","02.01.2006-15:04","2006-01-02","2006-01-02-07:00","2006-01-02T15:04:05.999999-07:00",}
func TryLayoutsToParseStringToTimeWithTZ(lps LoggedParseString, importLayouts []string) (time.Time, error) {
	localtime, _ := time.LoadLocation(Location)
	time.Local = localtime
	for _, importLayout := range importLayouts {
		newTime, err := time.ParseInLocation(importLayout, lps.s, localtime)

		if err == nil {
			// fmt.Printf("String: %s got parsed to: %v \n", s, newTime)
			return newTime, err
		}
	}
	storeFailure("'"+lps.s+"' asTime with TimeZone", lps.conFailStat)
	return time.Time{}, errors.New("Could not Parse with this ImportLayouts")
}

//TryLayoutsToParseStringToTime decides depending on the parameter to trys to parse the string within LoggedParseString
// to time.Time with or without timezone information
// With tz: []string{"02.01.2006-03:04","02.01.2006-15:04","2006-01-02","2006-01-02-07:00","2006-01-02T15:04:05.999999-07:00",}
// Without tz: []TimeZoneLayoutToRemove{TimeZoneLayoutToRemove{"2006-01-02", "-07:00"},	TimeZoneLayoutToRemove{"15:04:05", "-07:00"},	}
func (lps LoggedParseString) TryLayoutsToParseStringToTime(i interface{}) (time.Time, error) {
	var newTime time.Time
	var err error
	switch v := i.(type) {
	case []string:
		newTime, err = TryLayoutsToParseStringToTimeWithTZ(lps, v)
		return newTime, err
	case []TimeLayoutTrimTimeZone:
		newTime, err = TryLayoutsToParseStringToTimeWithoutTZ(lps, v)
		return newTime, err
	default:
		err = errors.New("Needs []string or []TimeLayoutTrimedTimeZone as Parameter")
	}
	return time.Time{}, err
}

// Pair used as p := Pair{"2006-01-02", "-07:00"} to define the tz format p[1]
// which shall be removed from a string so that the resulting string has the format p[0]
// so that string can be parsed as time
type Pair [2]interface{}

//ParseStringToTime trys to parse a string as time.Time and logs failures in conFailStat
func ParseStringToTime(s string, conFailStat *sync.Map) time.Time {
	newTime := time.Time{}
	s = strings.TrimSpace(s)
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
}

//ParseStringsDateAndTimeToTimestamp merges date and time strings into a new time
func ParseStringsDateAndTimeToTimestamp(dateString string, timeString string, conFailStat *sync.Map) *tspb.Timestamp {
	dateLPS := LoggedParseString{dateString, conFailStat}
	timeLPS := LoggedParseString{timeString, conFailStat}
	newLPS, err := MergeStringDateAndTime(dateLPS, timeLPS)
	if err != nil {
		storeFailure("'"+dateString+" "+timeString+"' asMergedTimestamp", conFailStat)
	}
	return newLPS.ParseStringToTimestamp()
}

//MergeStringDateAndTime will combine given date string and time string using dates offset omitting times offset
func MergeStringDateAndTime(dateLPS LoggedParseString, timeLPS LoggedParseString) (LoggedParseString, error) {
	var err error
	newLPS := LoggedParseString{conFailStat: dateLPS.conFailStat}
	dateTltt := TimeLayoutTrimTimeZone{"2006-01-02", "-07:00"}
	dateStringPrefix, dateTZ, dateErr := dateLPS.splitTZ(dateTltt)
	if dateErr != nil {
		return newLPS, errors.New("Date TZ could not be split")
	}
	timeTltt := TimeLayoutTrimTimeZone{"15:04:05", "-07:00"}
	timeStringPrefix, _, timeErr := timeLPS.splitTZ(timeTltt)
	if timeErr != nil {
		return newLPS, errors.New("Time TZ could not be split")
	}
	newLPS.s = strings.Join([]string{dateStringPrefix, timeStringPrefix, dateTZ}, " ")

	return newLPS, err
}
