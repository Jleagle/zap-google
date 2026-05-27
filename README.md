# zap-google

[![Go Reference](https://pkg.go.dev/badge/github.com/Jleagle/zap-google.svg)](https://pkg.go.dev/github.com/Jleagle/zap-google)
[![Go Report Card](https://goreportcard.com/badge/github.com/Jleagle/zap-google)](https://goreportcard.com/report/github.com/Jleagle/zap-google)

## Installation

```bash
go get github.com/Jleagle/zap-google
```

## Usage

```go
package main

import (
	"github.com/Jleagle/zap-google"
	"go.uber.org/zap"
)

func main() {
	core, err := zapgoogle.NewCore("your-project-id", false, nil, nil)
	if err != nil {
		panic(err)
	}

	logger := zap.New(core)
	defer logger.Sync()

	logger.Info("hello world", zap.String("foo", "bar"))
}
```
