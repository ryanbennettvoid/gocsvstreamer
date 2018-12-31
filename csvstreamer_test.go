package gocsvstreamer

import (
  "context"
  "fmt"
  "testing"
  "time"

  "github.com/stretchr/testify/assert"
)

const TEST_CSV_URL = "https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv?accessType=DOWNLOAD"

func TestStream(t *testing.T) {

  assert := assert.New(t)

  streamer := New()
  streamer.Url = TEST_CSV_URL
  assert.Nil(streamer.Listeners[EVENT_LINE])
  streamer.On(EVENT_LINE, func(data interface{}) {
    if line, ok := data.(Line); ok {
      fmt.Println(line.AsString())
    }
  })
  assert.NotNil(streamer.Listeners[EVENT_LINE])
  assert.Len(streamer.Listeners[EVENT_LINE], 1)

  ctx, cancel := context.WithCancel(context.Background())
  go func() {
    assert.NoError(streamer.Run(ctx))
  }()
  time.Sleep(5 * time.Second)
  cancel()

}
