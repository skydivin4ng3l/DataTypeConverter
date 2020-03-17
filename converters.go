package DataTyeConverter

import (
	"strconv"
	"strings"
	"sync"
	"time"

	ptypes "github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	log "github.com/sirupsen/logrus"
)

func storeFailiure(unparseable string, conFailStat *sync.Map) {
	counter, ok := conFailStat.Load(unparseable)
	if ok {
		conFailStat.Store(unparseable, counter.(int64)+1)
	} else {
		conFailStat.Store(unparseable, 1)
	}
}

func PrintFailStat(conFailStat *sync.Map) {
	conFailStat.Range(func(unparseable, counter interface{}) bool {
		log.Infof("Was not able to parse: ", unparseable, " ", counter, " times")
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
		storeFailiure(s, conFailStat)
		return 0
	}
	return number
}

func ParseStringToInt64(s string, conFailStat *sync.Map) int64 {
	number, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		storeFailiure(s, conFailStat)
		return 0
	}
	return number
}

//this is copied form the tmpmodels
func ToTimestamp(t time.Time) *tspb.Timestamp {
	ts, _ := ptypes.TimestampProto(t)
	return ts
}

func YYYYMMDDHHMMSSToTime(s string) time.Time {
	year, _ := strconv.Atoi(s[0:4])
	monthNumber, _ := strconv.Atoi(s[5:7])
	month := time.Month(monthNumber)
	day, _ := strconv.Atoi(s[8:10])
	hour, _ := strconv.Atoi(s[11:13])
	minute, _ := strconv.Atoi(s[14:16])
	second, _ := strconv.Atoi(s[17:19])
	nanoseconds := 0
	location, _ := time.LoadLocation("Europe/Berlin")

	return time.Date(year, month, day, hour, minute, second, nanoseconds, location)
}

func NumberDateToTime(s string) time.Time {
	year, _ := strconv.Atoi(s[0:4])
	monthNumber, _ := strconv.Atoi(s[4:6])
	month := time.Month(monthNumber)
	day, _ := strconv.Atoi(s[6:8])
	hour, _ := strconv.Atoi(s[8:10])
	minute, _ := strconv.Atoi(s[10:12])
	second, _ := strconv.Atoi(s[12:14])
	nanoseconds := 0
	location, _ := time.LoadLocation("Europe/Berlin")

	return time.Date(year, month, day, hour, minute, second, nanoseconds, location)
}

// 28-APR-19
func EletaDateToTimestamp(s string, conFailStat *sync.Map) *tspb.Timestamp {
	importLayout := "02-Jan-06"

	newTimestamp, err := time.Parse(importLayout, s)
	if err != nil {
		// fmt.Println("Not able to parse time:", err)
		storeFailiure(s, conFailStat)
		return ToTimestamp(time.Time{})

	}
	// log.Debug(newTimestamp, ToTimestamp(newTimestamp))
	return ToTimestamp(newTimestamp)
}

//01-APR-19 03.12.00.000000000 PM +02:00
//01-APR-19 03.12.00.000000000 PM GMT
//01-APR-19 03.12.00 PM +02:00
func EletaTimestampToTimestamp(s string, conFailStat *sync.Map) *tspb.Timestamp {
	importLayout := "02-Jan-06 03.04.05.000000000 PM -07:00"
	newTimestamp, err := time.Parse(importLayout, s)
	if err != nil {
		importLayoutNoNano := "02-Jan-06 03.04.05 PM -07:00"
		newTimestamp, err = time.Parse(importLayoutNoNano, s)
		if err != nil {
			importLayoutTimezone := "02-Jan-06 03.04.05.000000000 PM MST"
			newTimestamp, err = time.Parse(importLayoutTimezone, s)
			if err != nil {
				// fmt.Println("Not able to parse time:", err)
				storeFailiure(s, conFailStat)
				return ToTimestamp(time.Time{})
			}
		}
	}
	return ToTimestamp(newTimestamp)
}
