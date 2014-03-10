package csvb

import (
	"bytes"
	"encoding/csv"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type Destination struct {
	Name    string
	Date    time.Time
	Counter int64
}

func TestRowBinding(t *testing.T) {
	header := []string{"n", "d", "c"}
	row := []string{"foo", "2014-04-06 10:02:21", "4459813"}
	input := [][]string{header, row}
	s := make(map[string]string)
	s["n"] = "Name"
	s["d"] = "Date"
	s["c"] = "Counter"
	d := Destination{
		Name:    "foo",
		Counter: 4459813,
		Date:    time.Date(2014, 4, 6, 10, 02, 21, 0, time.UTC),
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
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	for _, row := range input {
		w.Write(row)
	}
	w.Flush()

	var d Destination

	opts := &Options{NullMarker: "NULL"}

	b := NewBinder(&buf, opts)
	b.ForEach(func(r Row) (bool, error) {
		if err := r.Bind(&d, s); err != nil {
			return false, err
		}
		return false, nil
	})

	assert.Equal(t, d, expected)
}
