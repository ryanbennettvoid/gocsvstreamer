package main

import (
  "context"
  "fmt"
  "strings"

  "github.com/ryanbennettvoid/gocsvstreamer"
  "github.com/ryanbennettvoid/gocsvstreamer/events"
)

func main() {

  ctx, cancel := context.WithCancel(context.Background())

  maxLines := 100000
  equifaxCounter := 0
  wellsfargoCounter := 0

  url := "https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv?accessType=DOWNLOAD"

  streamer := gocsvstreamer.New()
  streamer.Url = url
  streamer.On(events.LINE, func(data interface{}) {
    if line, ok := data.(gocsvstreamer.Line); ok {

      company := strings.ToLower(line.Data["Company"].(string))

      if strings.Contains(company, "equifax") {
        equifaxCounter++
      } else if strings.Contains(company, "wells fargo") {
        wellsfargoCounter++
      }

      if streamer.NumRowsProcessed%10000 == 0 {
        fmt.Printf("complaints found for Equifax: %d\n", equifaxCounter)
        fmt.Printf("complaints found for Wells Fargo: %d\n", wellsfargoCounter)
        fmt.Printf("num lines processed: %d\n\n", streamer.NumRowsProcessed)
      }

      if streamer.NumRowsProcessed >= maxLines {
        fmt.Printf("reached max number of lines (%d)\n", maxLines)
        cancel()
      }

    }
  })

  if err := streamer.Run(ctx); err != nil {
    panic(err)
  }

}
