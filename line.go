package gocsvstreamer

import "fmt"

type Line struct {
  Columns []string
  Data    map[string]interface{}
}

func NewLine() Line {
  return Line{
    Data: make(map[string]interface{}),
  }
}

func (line *Line) AsString() string {
  str := ""
  for _, column := range line.Columns {
    str += fmt.Sprintf("(%s:%s) ", column, line.Data[column])
  }
  return str
}
