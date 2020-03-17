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
		var once int64 = 1
		conFailStat.Store(unparseable, once)
	}
}

func PrintFailStat(conFailStat *sync.Map) {
	conFailStat.Range(func(unparseable, counter interface{}) bool {
		log.Infof("Was NOT able to parse: '%s'  %d times!", unparseable.(string), counter.(int64))
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

//01-APR-19 03.12.00.000000000 PM +02:00
//01-APR-19 03.12.00 PM +02:00
//01-APR-19 03.12.00.000000000 PM GMT
//01-APR-19 03.12.00 PM +02:00
//20181231231649+0000 YYYYMMDDHHMMSS+0000
func ParseStringToTimestamp(s string, conFailStat *sync.Map) *tspb.Timestamp {
	importLayouts := []string{
		"02-Jan-06",
		"02-01-2006",
        	"02.01.2006-03:04",
		"20060102030405-0700",
		"02-Jan-06 03.04.05 PM -07:00",
		"02-Jan-06 03.04.05.000000000 PM MST",
		"02-Jan-06 03.04.05.000000000 PM -07:00",
	}
	for _, importLayout := range importLayouts {
		newTimestamp, err := time.Parse(importLayout, s)
		if err == nil {
			return ToTimestamp(newTimestamp)
		}
	}
	storeFailiure(s, conFailStat)
	return ToTimestamp(time.Time{})
}
