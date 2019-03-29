
# Go CSV Streamer

``` go
streamer := gocsvstreamer.New()
streamer.Url = "http://super-big-file.csv"
streamer.On(events.LINE, func(data interface{}) {
  if line, ok := data.(gocsvstreamer.Line); ok {
    // do something with the line
  }
})
err := streamer.Run(context.Background())
``` 
