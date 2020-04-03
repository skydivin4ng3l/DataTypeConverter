package datatypeconverter

import (
	"sync"
	"testing"
)

func TestParseStringToInt64(t *testing.T) {
	var failStat sync.Map
	bad := ParseStringToInt64("not good", &failStat)
	good := ParseStringToInt64(" 12", &failStat)
	badField := ParseStringToInt64("12dot12", &failStat, "my field name")
	badFields := ParseStringToInt64("1a", &failStat, "my field name again", "i am ignored")

	if bad+badField+badFields != 0 {
		t.Errorf("Parsing of non numeric string to int64 did not yield 0 as expected")
	}
	if good != 12 {
		t.Errorf("Parsing of numeric string to int64 did not succeed")
	}
	PrintFailStat(&failStat)
}
