package DataTyeConverter

import (
	"math"
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
		return math.MaxFloat64
	}
	return number
}

func ParseStringToInt64(s string, conFailStat *sync.Map) int64 {
	number, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		storeFailiure(s, conFailStat)
		return math.MinInt64
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
	localtime, _ := time.LoadLocation("Europe/Berlin")
	time.Local = localtime
	importLayouts := []string{
		// for layout information: https://yourbasic.org/golang/format-parse-string-time-date-example/
		"02-Jan-06",
		"02-01-2006",
		"02.01.2006-03:04",
		"2016-01-02",
		"02.01.2006-03:04",
		"2006-01-02 03:04:05.999",
		"20060102030405-0700",
		"02-Jan-06 03.04.05 PM -07:00",
		"02-Jan-06 03.04.05.000000000 PM MST",
		"02-Jan-06 03.04.05.000000000 PM -07:00",
		"20060102030405",
		"20060102",
	}
	for _, importLayout := range importLayouts {
		newTimestamp, err := time.Parse(importLayout, s)
		if err == nil {
			// fmt.Printf("String: %s got parsed to: %v \n", s, newTimestamp)
			return ToTimestamp(newTimestamp)
		}
	}
	storeFailiure(s, conFailStat)
	return ToTimestamp(time.Time{})
}
