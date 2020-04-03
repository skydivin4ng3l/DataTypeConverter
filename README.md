# DataTypeConverter



## Null Values or not parseable

Our loaders interpret Null values or not parse-able strings:

### Int64
If strings can't be parsed into **TYPE** it will be set to _VALUE_ instead to reserve 0 to actual 0 values

 `int64 -> math.MinInt64 = -9223372036854775808`

Additionally int gets parsed as decimal if normal int parsing fails (some tables stabe the delay as 10.0 instead of 10 for a 10 min delay)
### Decimal
 `decimal.Decimal -> math.MinInt64.math.MinInt32 = -9223372036854775808.2147483648`

### Float64
`float64 -> math.MaxFloat64 = 1.7976931348623157e+308`

### Time
`time -> time.time{}  = January 1, year 1, 00:00:00 UTC`

### Timestamps
`timestamp -> nil`

