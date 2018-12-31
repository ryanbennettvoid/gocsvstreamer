package main

import (
  "context"
  "fmt"

  "github.com/ryanbennettvoid/gocsvstreamer"
)

func main() {

  url := "https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv?accessType=DOWNLOAD"

  streamer := gocsvstreamer.New()
  streamer.Url = url
  streamer.On(gocsvstreamer.EVENT_LINE, func(data interface{}) {
    if _, ok := data.(gocsvstreamer.Line); ok {
      if streamer.NumLinesProcessed%10000 == 0 {
        fmt.Printf("processed %d lines\n", streamer.NumLinesProcessed)
      }
    }
  })
  err := streamer.Run(context.Background())
  if err != nil {
    panic(err)
  }

}
