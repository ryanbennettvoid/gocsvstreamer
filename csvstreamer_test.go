package gocsvstreamer

import (
  "context"
  "testing"
  "time"

  "github.com/ryanbennettvoid/gocsvstreamer/events"
  "github.com/stretchr/testify/assert"
)

const CSV_LARGE = "https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv?accessType=DOWNLOAD"
const CSV_SMALL = "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD"

func TestStreamEvents(t *testing.T) {

  linesCounted := 0
  didEof := false

  assert := assert.New(t)

  streamer := New()
  streamer.Url = CSV_SMALL

  // LINE event
  assert.Nil(streamer.Listeners[events.LINE])
  streamer.On(events.LINE, func(data interface{}) {
    if _, ok := data.(Line); ok {
      linesCounted++
    }
  })
  assert.NotNil(streamer.Listeners[events.LINE])
  assert.Len(streamer.Listeners[events.LINE], 1)

  // EOF event
  assert.Nil(streamer.Listeners[events.EOF])
  streamer.On(events.EOF, func(_ interface{}) {
    didEof = true
  })
  assert.NotNil(streamer.Listeners[events.EOF])
  assert.Len(streamer.Listeners[events.EOF], 1)

  assert.NoError(streamer.Run(context.Background()))

  assert.Equal(236, linesCounted)
  assert.Equal(236, streamer.NumRowsProcessed)
  assert.True(didEof)
}

func TestStreamTimeout(t *testing.T) {

  assert := assert.New(t)

  started := time.Time{}

  streamer := New()
  streamer.Url = CSV_LARGE
  streamer.On(events.LINE, func(data interface{}) {
    if _, ok := data.(Line); ok {
      if started.IsZero() {
        started = time.Now()
      }
    }
  })
  ctx, _ := context.WithTimeout(context.Background(), (time.Second * 5))
  assert.NoError(streamer.Run(ctx))

  assert.True(time.Since(started) < ((time.Second * 5) + (time.Millisecond * 100)))

}
