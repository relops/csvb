package csvb

import (
	"encoding/csv"
	"github.com/oleiade/reflections"
	"io"
	"reflect"
	"strconv"
	"time"
)

type Options struct {
	Separator  rune
	NullMarker string
}

type Binder struct {
	csv  *csv.Reader
	meta map[int]string
	opts *Options
}

type Row struct {
	data map[string]string
}

func NewBinder(reader io.Reader, opts *Options) *Binder {

	csv := csv.NewReader(reader)

	if opts == nil {
		opts = &Options{}
	} else {
		if opts.Separator == 0 {
			opts.Separator = ','
		}
		csv.Comma = opts.Separator
	}

	header, _ := csv.Read()

	meta := make(map[int]string)
	for i, col := range header {
		meta[i] = col
	}

	return &Binder{csv: csv, meta: meta, opts: opts}
}

func (b *Binder) ReadRow() (Row, error) {
	row, err := b.csv.Read()
	if err != nil {
		return Row{}, err
	}
	data := make(map[string]string)
	for i, v := range row {
		if len(v) > 0 && v != b.opts.NullMarker {
			k := b.meta[i]
			data[k] = v
		}
	}
	return Row{data: data}, nil
}

func (b *Binder) ForEach(f func(Row) bool) error {

	for {
		row, err := b.ReadRow()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if !f(row) {
			break
		}
	}

	return nil
}

func (r *Row) Bind(x interface{}, strategy map[string]string) error {

	for src, dest := range strategy {

		data, ok := r.data[src]

		if ok {
			k, err := reflections.GetFieldKind(x, dest)
			if err != nil {
				return err
			}

			switch k {
			case reflect.String:
				{
					reflections.SetField(x, dest, data)
				}
			case reflect.Int64:
				{
					i, err := strconv.ParseInt(data, 10, 64)
					if err != nil {
						return err
					}
					reflections.SetField(x, dest, i)
				}
			case reflect.Struct:
				{
					value, err := reflections.GetField(x, dest)
					if err != nil {
						return err
					}
					if reflect.TypeOf(value) == reflect.TypeOf(time.Now()) {
						date, err := time.Parse("2006-01-02 15:04:05", data)
						if err != nil {
							return err
						}
						reflections.SetField(x, dest, date)
					}
				}
			}
		}
	}

	return nil
}
