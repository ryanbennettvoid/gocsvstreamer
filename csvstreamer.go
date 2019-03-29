package gocsvstreamer

import (
  "context"
  "encoding/csv"
  "errors"
  "net/http"

  "github.com/ryanbennettvoid/gocsvstreamer/events"
)

type CsvStreamer struct {
  Url              string
  Listeners        map[string][]Callback
  Columns          []string
  NumRowsProcessed int
  started          bool
}

func New() CsvStreamer {
  return CsvStreamer{
    Listeners: make(map[string][]Callback),
  }
}

func (streamer *CsvStreamer) On(eventName string, fn Callback) {
  if _, ok := streamer.Listeners[eventName]; !ok {
    streamer.Listeners[eventName] = []Callback{}
  }
  streamer.Listeners[eventName] = append(streamer.Listeners[eventName], fn)
}

func (streamer *CsvStreamer) Emit(eventName string, data interface{}) {
  if _, ok := streamer.Listeners[eventName]; !ok {
    return
  }
  for i := 0; i < len(streamer.Listeners[eventName]); i++ {
    streamer.Listeners[eventName][i](data)
  }
}

func (streamer *CsvStreamer) Run(ctx context.Context) error {
  if streamer.started {
    return errors.New("already started")
  }
  streamer.started = true
  if len(streamer.Url) == 0 {
    return errors.New("url is missing")
  }
  res, err := http.Get(streamer.Url)
  if err != nil {
    return err
  }
  streamer.NumRowsProcessed = 0
  csvReader := csv.NewReader(res.Body)
  for {
    select {
    case <-ctx.Done():
      return nil
    default:
      record, err := csvReader.Read()
      if err != nil {
        if err.Error() == "EOF" {
          streamer.Emit(events.EOF, nil)
          return nil
        }
        return err
      }
      if streamer.Columns == nil {
        streamer.Columns = record
      } else {
        line := NewLine()
        line.Columns = streamer.Columns
        for i, column := range streamer.Columns {
          line.Data[column] = record[i]
        }
        streamer.NumRowsProcessed++
        streamer.Emit(events.LINE, line)
      }
    }
  }
  return nil
}
