package zapgoogle

import (
	"context"
	"encoding/json"
	"strings"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//goland:noinspection GoUnusedExportedFunction
func New(ctx context.Context, logger *logging.Logger, synchronous bool) zapcore.Core {

	return &googleCore{
		logger:      logger,
		context:     ctx,
		synchronous: synchronous,
		encoder: zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			MessageKey:     "message",
			LevelKey:       "severity",
			TimeKey:        "timestamp",
			NameKey:        "logName",
			CallerKey:      "caller",
			StacktraceKey:  "stack_trace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.RFC3339TimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}),
	}
}

type googleCore struct {
	logger      *logging.Logger
	context     context.Context
	synchronous bool
	encoder     zapcore.Encoder
}

func (g *googleCore) Enabled(_ zapcore.Level) bool {
	return true
}

func (g *googleCore) With(fields []zapcore.Field) zapcore.Core {

	clone := &googleCore{
		logger:      g.logger,
		context:     g.context,
		synchronous: g.synchronous,
		encoder:     g.encoder.Clone(),
	}

	for k := range fields {
		fields[k].AddTo(clone.encoder)
	}

	return clone
}

func (g *googleCore) Check(entry zapcore.Entry, checkedEntry *zapcore.CheckedEntry) *zapcore.CheckedEntry {

	if g.Enabled(entry.Level) {
		return checkedEntry.AddCore(entry, g)
	}

	return checkedEntry
}

func (g *googleCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {

	if entry.Level >= zapcore.ErrorLevel {
		newFields := make([]zapcore.Field, 0, len(fields)+1)
		newFields = append(newFields, fields...)
		newFields = append(newFields, zap.String("@type", "type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent"))
		fields = newFields

		if entry.Stack != "" {
			// Format zap's stack trace to look like a standard Go panic stack trace
			// so Error Reporting can parse it correctly.
			stack := entry.Stack
			lines := strings.Split(stack, "\n")
			for i, line := range lines {
				if !strings.HasPrefix(line, "\t") && line != "" {
					if !strings.HasSuffix(line, ")") {
						lines[i] = line + "()"
					}
				}
			}
			entry.Stack = "goroutine 1 [running]:\n" + strings.Join(lines, "\n")
		}
	}

	buf, err := g.encoder.EncodeEntry(entry, fields)
	if err != nil {
		return err
	}

	var level logging.Severity

	switch entry.Level {
	case zapcore.DebugLevel:
		level = logging.Debug
	case zapcore.InfoLevel:
		level = logging.Info
	case zapcore.WarnLevel:
		level = logging.Warning
	case zapcore.ErrorLevel:
		level = logging.Error
	case zapcore.DPanicLevel:
		level = logging.Critical
	case zapcore.PanicLevel:
		level = logging.Critical
	case zapcore.FatalLevel:
		level = logging.Alert
	default:
		level = logging.Default
	}

	b := make([]byte, buf.Len())
	copy(b, buf.Bytes())

	buf.Free()

	googleEntry := logging.Entry{
		Timestamp: entry.Time,
		Severity:  level,
		Payload:   json.RawMessage(b),
	}

	if entry.Caller.Defined {
		googleEntry.SourceLocation = &loggingpb.LogEntrySourceLocation{
			File:     entry.Caller.File,
			Line:     int64(entry.Caller.Line),
			Function: entry.Caller.Function,
		}
	}

	if g.synchronous {
		return g.logger.LogSync(g.context, googleEntry)
	}

	g.logger.Log(googleEntry)

	return nil
}

func (g *googleCore) Sync() error {
	return g.logger.Flush()
}
