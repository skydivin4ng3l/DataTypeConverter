package DataTyeConverter

import (
	"fmt"
	"strconv"
	"time"

	ptypes "github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
)

// this converts the J, B notation of bools to go bools
func ToBool(s string) bool {
	if s == "J" {
		return true
	} else {
		return false
	}
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
func EletaDateToTimestamp(s string) *tspb.Timestamp {
	importLayout := "02-Jan-06"

	newTimestamp, err := time.Parse(importLayout, s)
	if err != nil {
		fmt.Println("Not able to parse time:", err)

	}
	// log.Debug(newTimestamp, ToTimestamp(newTimestamp))
	return ToTimestamp(newTimestamp)
}

//01-APR-19 03.12.00.000000000 PM +02:00
//01-APR-19 03.12.00.000000000 PM GMT
//01-APR-19 03.12.00 PM +02:00
func EletaTimestampToTimestamp(s string) *tspb.Timestamp {
	importLayout := "02-Jan-06 03.04.05.000000000 PM -07:00"
	// AlternateLayout := "02-Jan-06 03.04.05.000000000 PM -07:00"
	// newlayout := "yyyy-mm-dd hh:mm:ss + nsec nanoseconds"
	newTimestamp, err := time.Parse(importLayout, s)
	if err != nil {
		importLayoutNoNano := "02-Jan-06 03.04.05 PM -07:00"
		newTimestamp, err = time.Parse(importLayoutNoNano, s)
		if err != nil {
			importLayoutTimezone := "02-Jan-06 03.04.05.000000000 PM MST"
			newTimestamp, err = time.Parse(importLayoutTimezone, s)
			if err != nil {
				fmt.Println("Not able to parse time:", err)
				return ToTimestamp(time.Time{})
			}
			// log.Debug(">>>>>>>>>>>>>>>>>>>>>>>>>>>> ", newTimestamp, " And the Timestamp: ", ToTimestamp(newTimestamp))
		}
	}
	return ToTimestamp(newTimestamp)
}
