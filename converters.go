package datatypeconverter

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	ptypes "github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/shopspring/decimal"
	"github.com/skydivin4ng3l/datatypeconverter/logger"
)

//Location the Location to Parse the Time in
var Location = "Europe/Berlin"

// TimeLayoutSplitTimeZone defines the tz format TZoneLayoutToSplit
// 	which shall be split from a string so that the resulting string has the format TLayoutPostTZSplit so that string can be parsed as time
// 	example TimeLayoutSplitTimeZone{"2006-01-02", "-07:00"}
type TimeLayoutSplitTimeZone struct {
	TLayoutPostTZSplit string
	TZoneLayoutToSplit string
}

//DefaultImportLayoutsWithoutTimeZone defines the time layouts used by default to parse strings without TimeZone suffix to time
// 	if a custome layouts []TimeLayoutSplitTimeZone is provided, it will be append by these Defaults to your own array if you need specific other combinations
// 	for layout information: https://yourbasic.org/golang/format-parse-string-time-date-example/
var DefaultImportLayoutsWithoutTimeZone = []TimeLayoutSplitTimeZone{
	TimeLayoutSplitTimeZone{"2006-01-02", "-07:00"},
	TimeLayoutSplitTimeZone{"15:04:05", "-07:00"},
}

//DefaultImportLayouts defines the time layouts used by default to parse strings to time
//	if a custome layouts []string is provided, it will be append by these Defaults to your own array if you need specific other strings
//	for layout information: https://yourbasic.org/golang/format-parse-string-time-date-example/
var DefaultImportLayouts = []string{
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
	"2006-01-02T15:04",
	"2006-01-02T15:04:00",
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
	splitTZ(TimeLayoutSplitTimeZone) (string, string, error)
}

//LoggedParseString a pair of string and a *sync.Map which stores parse erros
type LoggedParseString struct {
	S           string
	ConFailStat *sync.Map
}

func (lps LoggedParseString) storeFailure() {
	logger.StoreFailure(lps.S, lps.ConFailStat)
}

// PrintFailStat !Legacy! prints Statistics of Failed LoggedParseString parsing to console and the LogFile
func PrintFailStat(conFailStat *sync.Map) {
	logger.PrintFailStat(conFailStat)
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
	return ParseStringToFloat64(lps.S, lps.ConFailStat)
}

// ParseStringToFloat64 prases the string s as float64 and logs any failures in conFailStat
func ParseStringToFloat64(s string, conFailStat *sync.Map) float64 {
	number, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {

		logger.StoreFailure("'"+s+"' asFloat64", conFailStat)
		return math.MaxFloat64
	}
	return number
}

// ParseStringToDecimal prases the string in LoggedParseString as decimal.Decimal and logs any failures
func (lps LoggedParseString) ParseStringToDecimal() decimal.Decimal {
	return ParseStringToDecimal(lps.S, lps.ConFailStat)
}

// ParseStringToDecimal prases the string s as decimal.Decimal and logs any failures in conFailStat
func ParseStringToDecimal(s string, conFailStat *sync.Map) decimal.Decimal {
	number, err := decimal.NewFromString(strings.TrimSpace(s))
	if err != nil {
		logger.StoreFailure("'"+s+"' asDecimal", conFailStat)
		return decimal.New(math.MinInt64, math.MinInt32)
	}
	return number
}

// ParseStringToInt64 parses the string in LoggedParseString as int64 and logs any failures
func (lps LoggedParseString) ParseStringToInt64() int64 {
	return ParseStringToInt64(lps.S, lps.ConFailStat)
}

// ParseStringToInt64 parses a string to an int64 and stores any failure
func ParseStringToInt64(s string, failStat *sync.Map, fields ...string) int64 {
	s = strings.Replace(s, "\n", "", -1) // Remove line breaks
	s = strings.Replace(s, " ", "", -1)  // Remove any spaces
	number, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		decimalNumber, err := decimal.NewFromString(s)
		if err != nil {
			logger.CheckForError(err, s, reflect.Int64, failStat, fields...)
			return math.MinInt64
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

// ParseStringToTimestamp parses the string in LoggedParseString as *tspb.Timestamp and logs any failures
func (lps LoggedParseString) ParseStringToTimestamp() *tspb.Timestamp {
	return ParseStringToTimestamp(lps.S, lps.ConFailStat)
}

// ParseStringToTimestamp pses the string in LoggedParseString as *tspb.Timestamp and logs any failures
func ParseStringToTimestamp(s string, conFailStat *sync.Map) *tspb.Timestamp {

	return ToTimestamp(ParseStringToTime(s, conFailStat))
}

// stringSplitTZAbbreviation removes Timezone Abbreviations seperated by " " like GMT from the string in LoggedParseString and logs any failures
// 	returns error if there is no Timezone defined
func (lps LoggedParseString) stringSplitTZAbbreviation() (string, string, error) {
	var err error
	lps.S = strings.TrimSpace(lps.S)
	substrings := strings.Split(lps.S, " ")
	substingsLen := len(substrings)
	tzSuffix := substrings[substingsLen-1]
	_, err = time.LoadLocation(tzSuffix)
	if err != nil {
		logger.StoreFailure("'"+lps.S+"' could not remove "+tzSuffix+" as TimeZone Abbreviation GMT", lps.ConFailStat)
		return "", "", err
	}
	sPrefix := strings.Join(substrings[:substingsLen-1], " ")
	sPrefix = strings.TrimSpace(sPrefix)
	return sPrefix, tzSuffix, err
}

// stringSplitTZOffset removes Timezone Offsets like -07 -0700 -07:00 from the string in LoggedParseString and logs any failures
// 	Cuts tz_suffix = '+/-07:00' || '+/-0700' || +/-07 from prefix+07:00 (7 exemplary)
// 	returns error if the cut part is short or longer than the tzSuffixLayout defined
func (lps LoggedParseString) stringSplitTZOffset(tzSuffixLayout string) (string, string, error) {
	return stringSplitTZOffset(lps, tzSuffixLayout)
}

// stringSplitTZOffset removes Timezone Offsets like -07 -0700 -07:00 from the string s and logs any failures in conFailStat
// 	Cuts tz_suffix = '+/-07:00' || '+/-0700' || +/-07 from prefix+07:00 (7 exemplary)
// 	returns error if the cut part is short or longer than the tzSuffixLayout defined
func stringSplitTZOffset(lps LoggedParseString, tzSuffixLayout string) (string, string, error) {
	var err error
	s := strings.TrimSpace(lps.S)
	sLength := len(s)
	var splits []string
	tzSuffix := ""
	sPrefix := ""
	delimiterSigns := []string{"+", "-"}
	for _, sign := range delimiterSigns {
		err = nil
		splits = strings.Split(s, sign)
		splitLength := len(splits)
		if splitLength < 2 {
			err = errors.New("TZ Suffix could not be found")
			continue
		}
		tzSuffix = sign + splits[splitLength-1]
		if len(tzSuffix) != len(tzSuffixLayout) {
			err = errors.New("TZ Suffix has not the Format " + tzSuffixLayout)
			tzSuffix = ""
			continue
		}
		sPrefix = strings.Join(splits[:splitLength-1], sign)
		break
	}

	if err != nil || len(sPrefix)+len(tzSuffix) != sLength {
		logger.StoreFailure("'"+lps.S+"' could not remove TimeZone with Format "+tzSuffixLayout, lps.ConFailStat)
	}
	sPrefix = strings.TrimSpace(sPrefix)
	return sPrefix, tzSuffix, err
}

//ParseStringToDate !Depricated! removes the timezone offset before parsing the string from LoggedParseString as *tspb.Timestamp and logging any failures
func (lps LoggedParseString) ParseStringToDate() *tspb.Timestamp {
	return ParseStringToDate(lps.S, lps.ConFailStat)
}

//ParseStringToDate !Depricated! removes the timezone offset before parsing the string s as *tspb.Timestamp and logging any failures to conFailStat
func ParseStringToDate(s string, conFailStat *sync.Map) *tspb.Timestamp {
	stringTZFree, _, err := stringSplitTZOffset(LoggedParseString{s, conFailStat}, "-07:00")
	if err != nil {
		logger.StoreFailure("'"+s+"' asDate", conFailStat)
		return nil
	}
	return ToTimestamp(ParseStringToTime(stringTZFree, conFailStat))
}

func (lps LoggedParseString) splitTZ(p TimeLayoutSplitTimeZone) (string, string, error) {
	var err error
	var sPrefix, tzSuffix string
	tzSuffixLayout := p.TZoneLayoutToSplit

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
//	for layout information: https://yourbasic.org/golang/format-parse-string-time-date-example/
// 	example:
// 	[]TimeZoneLayoutToRemove{
// 	TimeZoneLayoutToRemove{"2006-01-02", "-07:00"},
// 	TimeZoneLayoutToRemove{"15:04:05", "-07:00"},}
func TryLayoutsToParseStringToTimeWithoutTZ(lps LoggedParseString, layouts []TimeLayoutSplitTimeZone) (time.Time, error) {
	layouts = append(layouts[:], DefaultImportLayoutsWithoutTimeZone[:]...)
	localtime, _ := time.LoadLocation(Location)
	time.Local = localtime
	for _, importLayoutPair := range layouts {
		stringTZFree, _, tzError := lps.splitTZ(importLayoutPair)
		if tzError == nil {
			newTime, err := time.ParseInLocation(importLayoutPair.TLayoutPostTZSplit, stringTZFree, localtime)
			if err == nil {
				return newTime, err
			}
		}
	}
	logger.StoreFailure("'"+lps.S+"' asTime without TimeZone", lps.ConFailStat)
	return time.Time{}, errors.New("Could not Parse without TZ with this ImportLayouts")
}

//TryLayoutsToParseStringToTimeWithTZ trys to parse the string within LoggedParseString to time.Time using the importLayouts []string
//	for layout information: https://yourbasic.org/golang/format-parse-string-time-date-example/
// 	example importLayouts:
//	[]string{
//	"02.01.2006-03:04",
// 	"02.01.2006-15:04",
// 	"2006-01-02",
// 	"2006-01-02-07:00",
// 	"2006-01-02T15:04:05.999999-07:00",}
func TryLayoutsToParseStringToTimeWithTZ(lps LoggedParseString, importLayouts []string) (time.Time, error) {
	importLayouts = append(importLayouts[:], DefaultImportLayouts[:]...)
	localtime, _ := time.LoadLocation(Location)
	time.Local = localtime
	for _, importLayout := range importLayouts {
		newTime, err := time.ParseInLocation(importLayout, lps.S, localtime)

		if err == nil {
			// fmt.Printf("String: %s got parsed to: %v \n", s, newTime)
			return newTime, err
		}
	}
	logger.StoreFailure("'"+lps.S+"' asTime with TimeZone", lps.ConFailStat)
	return time.Time{}, errors.New("Could not Parse with this ImportLayouts")
}

//TryLayoutsToParseStringToTime decides depending on the parameter to trys to parse the string within LoggedParseString
// to time.Time with or without timezone information
//
// Needs Arrays of Layouts to work:
// 	Custom arrays of string{"yourlayout"} TimeZoneLayoutToRemove{"yourlayout","yourTZ"} will be appended with DefaultImportLayouts and DefaultImportLayoutsWithoutTimeZone respectivly
//	Examples
//	With tz:
//	[]string{"02.01.2006-03:04", "02.01.2006-15:04","2006-01-02","2006-01-02-07:00","2006-01-02T15:04:05.999999-07:00",}
// 	Without tz:
//	[]TimeZoneLayoutToRemove{TimeZoneLayoutToRemove{"2006-01-02", "-07:00"}, TimeZoneLayoutToRemove{"15:04:05", "-07:00"},}
func (lps LoggedParseString) TryLayoutsToParseStringToTime(i interface{}) (time.Time, error) {
	var newTime time.Time
	var err error
	switch v := i.(type) {
	case []string:
		newTime, err = TryLayoutsToParseStringToTimeWithTZ(lps, v)
		return newTime, err
	case []TimeLayoutSplitTimeZone:
		newTime, err = TryLayoutsToParseStringToTimeWithoutTZ(lps, v)
		return newTime, err
	default:
		err = errors.New("Needs at least an empty []string or []TimeLayoutSplitTimeZone as Parameter")
	}
	return time.Time{}, err
}

//TryLayoutsToParseStringToTime decides depending on the parameter to trys to parse the string s
// to time.Time with or without timezone information and will log errors to conFailStat
//
// Needs Arrays of Layouts to work:
// 	Custom arrays of string{"yourlayout"} TimeZoneLayoutToRemove{"yourlayout","yourTZ"} will be appended with DefaultImportLayouts and DefaultImportLayoutsWithoutTimeZone respectivly
//	Examples
//	With tz:
//	[]string{"02.01.2006-03:04", "02.01.2006-15:04","2006-01-02","2006-01-02-07:00","2006-01-02T15:04:05.999999-07:00",}
// 	Without tz:
//	[]TimeZoneLayoutToRemove{TimeZoneLayoutToRemove{"2006-01-02", "-07:00"}, TimeZoneLayoutToRemove{"15:04:05", "-07:00"},}
func TryLayoutsToParseStringToTime(s string, conFailStat *sync.Map, i interface{}) time.Time {
	lps := LoggedParseString{s, conFailStat}
	newTime, err := lps.TryLayoutsToParseStringToTime(i)
	if err != nil {
		logger.StoreFailure("'"+s+"' asTime because "+err.Error(), conFailStat)
	}
	return newTime
}

//ParseStringToTime trys to parse a string as time.Time and logs failures in conFailStat
//	defaults to time.Time{} if not parseable
func ParseStringToTime(s string, conFailStat *sync.Map) time.Time {
	newTime := time.Time{}
	var err error
	s = strings.TrimSpace(s)
	if s != "" {
		loggedParseString := LoggedParseString{s, conFailStat}
		newTime, err = loggedParseString.TryLayoutsToParseStringToTime([]string{})
		if err != nil {
			logger.StoreFailure("'"+s+"' asTime", conFailStat)
		}
	} else {
		logger.StoreFailure("'"+s+"' asTime", conFailStat)
	}
	return newTime
}

//ParseStringWithoutTZToTime trys to remove the timezone before parsing as time
//	defaults to time.Time{} if not parseable
func ParseStringWithoutTZToTime(s string, conFailStat *sync.Map) time.Time {
	s = strings.TrimSpace(s)
	newTime := time.Time{}
	var err error
	if s != "" {
		loggedParseString := LoggedParseString{s, conFailStat}
		newTime, err = loggedParseString.TryLayoutsToParseStringToTime([]TimeLayoutSplitTimeZone{})
		if err != nil {
			logger.StoreFailure("'"+s+"' asTime without TZ", conFailStat)
		}
	}
	return newTime
}

//ParseStringsDateAndTimeToTimestamp merges date and time strings into a new time
//	Currently only supports following layouts:
//	Date ("2006-01-02", "-07:00")
//	Time ("15:04:05", "-07:00")
// 	between the comma sperated layout parts can be spaces, offsets can also be positiv
func ParseStringsDateAndTimeToTimestamp(dateString string, timeString string, conFailStat *sync.Map) *tspb.Timestamp {
	dateLPS := LoggedParseString{dateString, conFailStat}
	timeLPS := LoggedParseString{timeString, conFailStat}
	newLPS, err := MergeStringDateAndTime(dateLPS, timeLPS)
	if err != nil {
		logger.StoreFailure("'"+dateString+" "+timeString+"' asMergedTimestampString", conFailStat)
	}
	return newLPS.ParseStringToTimestamp()
}

//MergeStringDateAndTime will combine given date string and time string using dates offset omitting the offset of time
//	Currently only supports following layouts:
//	Date ("2006-01-02", "-07:00")
//	Time ("15:04:05", "-07:00")
// 	between the comma sperated layout parts can be spaces, offsets can also be positiv
func MergeStringDateAndTime(dateLPS LoggedParseString, timeLPS LoggedParseString) (LoggedParseString, error) {
	var err error
	newLPS := LoggedParseString{ConFailStat: dateLPS.ConFailStat}
	dateTltt := TimeLayoutSplitTimeZone{"2006-01-02", "-07:00"}
	dateStringPrefix, dateTZ, dateErr := dateLPS.splitTZ(dateTltt)
	if dateErr != nil {
		return newLPS, errors.New("Date TZ could not be split")
	}
	timeTltt := TimeLayoutSplitTimeZone{"15:04:05", "-07:00"}
	timeStringPrefix, _, timeErr := timeLPS.splitTZ(timeTltt)
	if timeErr != nil {
		return newLPS, errors.New("Time TZ could not be split")
	}
	newLPS.S = strings.Join([]string{dateStringPrefix, timeStringPrefix, dateTZ}, " ")

	return newLPS, err
}
