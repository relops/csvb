package csvb

import (
	"bytes"
	"encoding/csv"
	"github.com/stretchr/testify/assert"
	"gopkg.in/inf.v0"
	"testing"
	"time"
)

type Destination struct {
	Name    string
	Date    time.Time
	Counter int64
	Rating  *inf.Dec
}

func TestVariableLengthFields(t *testing.T) {
	header := []string{"n", "d", "c", "x"}
	row := []string{"foo", "2014-04-06 10:02:21", "9834"}
	input := [][]string{header, row}
	s := make(map[string]string)
	s["n"] = "Name"
	s["d"] = "Date"
	s["c"] = "Counter"
	d := Destination{
		Name:    "foo",
		Counter: 9834,
		Date:    time.Date(2014, 4, 6, 10, 02, 21, 0, time.UTC),
	}
	runScenario(t, input, s, d)

}

func TestCustomHeader(t *testing.T) {
	header := map[int]string{
		0: "n",
		1: "d",
		2: "c",
	}
	row := []string{"foo", "2014-04-06 10:02:21", "4459813"}
	input := [][]string{row}
	s := make(map[string]string)
	s["n"] = "Name"
	s["d"] = "Date"
	s["c"] = "Counter"

	opts := &Options{Header: header}

	d := Destination{
		Name:    "foo",
		Counter: 4459813,
		Date:    time.Date(2014, 4, 6, 10, 02, 21, 0, time.UTC),
	}
	runScenarioWithOptions(t, input, s, d, opts)

}

func TestTimezoneHandling(t *testing.T) {
	header := []string{"n", "d", "c"}
	row := []string{"foo", "2014-04-06 10:02:21", "4459813"}
	input := [][]string{header, row}
	s := make(map[string]string)
	s["n"] = "Name"
	s["d"] = "Date"
	s["c"] = "Counter"

	location, _ := time.LoadLocation("Europe/Stockholm")
	opts := &Options{TimeZone: location}

	d := Destination{
		Name:    "foo",
		Counter: 4459813,
		Date:    time.Date(2014, 4, 6, 10, 02, 21, 0, location),
	}
	runScenarioWithOptions(t, input, s, d, opts)
}

func TestRowBinding(t *testing.T) {
	header := []string{"n", "d", "c", "r"}
	row := []string{"foo", "2014-04-06 10:02:21", "4459813", "1.55"}
	input := [][]string{header, row}
	s := make(map[string]string)
	s["n"] = "Name"
	s["d"] = "Date"
	s["c"] = "Counter"
	s["r"] = "Rating"
	d := Destination{
		Name:    "foo",
		Counter: 4459813,
		Date:    time.Date(2014, 4, 6, 10, 02, 21, 0, time.UTC),
		Rating:  inf.NewDec(155, 2),
	}
	runScenario(t, input, s, d)
}

func TestNullHandling(t *testing.T) {
	header := []string{"n", "d", "c"}
	row := []string{"foo", "2014-04-06 10:02:21", "NULL"}
	input := [][]string{header, row}
	s := make(map[string]string)
	s["n"] = "Name"
	s["d"] = "Date"
	s["c"] = "Counter"
	d := Destination{
		Name:    "foo",
		Counter: 0,
		Date:    time.Date(2014, 4, 6, 10, 02, 21, 0, time.UTC),
	}
	runScenario(t, input, s, d)
}

func runScenario(t *testing.T, input [][]string, s map[string]string, expected interface{}) {
	runScenarioWithOptions(t, input, s, expected, nil)
}

func runScenarioWithOptions(t *testing.T, input [][]string, s map[string]string, expected interface{}, opts *Options) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	for _, row := range input {
		w.Write(row)
	}
	w.Flush()

	var d Destination

	if opts == nil {
		opts = &Options{NullMarker: "NULL"}
	}

	b, err := NewBinder(&buf, opts)
	assert.NoError(t, err)

	b.ForEach(func(r Row) (bool, error) {
		if err := r.Bind(&d, s); err != nil {
			return false, err
		}
		return false, nil
	})

	assert.Equal(t, d, expected)
}
