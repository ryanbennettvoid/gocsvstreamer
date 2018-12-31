package gocsvstreamer

import (
  "context"
  "encoding/csv"
  "errors"
  "net/http"
)

type CsvStreamer struct {
  Url               string
  Listeners         map[string][]Callback
  Columns           []string
  NumLinesProcessed int
  Started           bool
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
  if streamer.Started {
    return errors.New("already started")
  }
  if len(streamer.Url) == 0 {
    return errors.New("url is missing")
  }
  res, err := http.Get(streamer.Url)
  if err != nil {
    return err
  }
  streamer.NumLinesProcessed = 0
  csvReader := csv.NewReader(res.Body)
  for {
    select {
    case <-ctx.Done():
      return nil
    default:
      record, err := csvReader.Read()
      if err != nil {
        return err
      }
      if streamer.NumLinesProcessed == 0 {
        streamer.Columns = record
      } else {
        if len(record) > len(streamer.Columns) {
          return errors.New("row length greater than number of columns")
        }
        line := NewLine()
        line.Columns = streamer.Columns
        for i, column := range streamer.Columns {
          line.Data[column] = record[i]
        }
        streamer.Emit(EVENT_LINE, line)
      }
      streamer.NumLinesProcessed++
    }
  }
  return nil
}
