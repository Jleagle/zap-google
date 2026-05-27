package zapgoogle

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"cloud.google.com/go/logging"
	"go.uber.org/zap/zapcore"
	"google.golang.org/api/option"
)

//goland:noinspection GoUnusedExportedFunction
func NewCore(projectID string, synchronous bool, clientOps []option.ClientOption, loggerOps []logging.LoggerOption) (zapcore.Core, error) {

	ctx := context.Background()

	googleClient, err := logging.NewClient(ctx, projectID, clientOps...)
	if err != nil {
		return nil, err
	}

	return NewCoreWithClient(googleClient, synchronous, loggerOps), nil
}

//goland:noinspection GoUnusedExportedFunction
func NewCoreWithClient(client *logging.Client, synchronous bool, loggerOps []logging.LoggerOption) zapcore.Core {

	return &googleCore{
		client:      client,
		context:     context.Background(),
		loggers:     map[string]*logging.Logger{},
		synchronous: synchronous,
		loggerOps:   loggerOps,
		mu:          &sync.RWMutex{},

		encoder: zapcore.NewJSONEncoder(googleEncoderConfig()),
		output:  zapcore.AddSync(io.Discard),
	}
}

type googleCore struct {
	client      *logging.Client
	context     context.Context
	loggers     map[string]*logging.Logger
	synchronous bool
	loggerOps   []logging.LoggerOption
	mu          *sync.RWMutex

	encoder zapcore.Encoder
	output  zapcore.WriteSyncer
}

func (g *googleCore) clone() *googleCore {

	return &googleCore{
		client:      g.client,
		context:     g.context,
		loggers:     g.loggers,
		synchronous: g.synchronous,
		mu:          g.mu,

		encoder: g.encoder.Clone(),
		output:  g.output,
	}
}

func (g *googleCore) getLogger(name string) *logging.Logger {

	g.mu.RLock()
	val, ok := g.loggers[name]
	g.mu.RUnlock()
	if ok {
		return val
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Double check
	if val, ok = g.loggers[name]; ok {
		return val
	}

	g.loggers[name] = g.client.Logger(name, g.loggerOps...)

	return g.loggers[name]
}

func (g *googleCore) Enabled(level zapcore.Level) bool {
	return true
}

func (g *googleCore) With(fields []zapcore.Field) zapcore.Core {

	clone := g.clone()
	for k := range fields {
		fields[k].AddTo(clone.encoder)
	}
	return clone
}

func (g *googleCore) Check(entry zapcore.Entry, checkedEntry *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return checkedEntry.AddCore(entry, g)
}

func (g *googleCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {

	buf, err := g.encoder.EncodeEntry(entry, fields)
	if err != nil {
		return err
	}

	var payload interface{}
	var payloadMap map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &payloadMap); err == nil {
		payload = payloadMap
	} else {
		payload = buf.String()
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
		level = logging.Error
	}

	googleEntry := logging.Entry{
		Timestamp: entry.Time,
		Severity:  level,
		Payload:   payload,
	}

	logger := g.getLogger(entry.LoggerName)

	if g.synchronous {
		return logger.LogSync(g.context, googleEntry)
	}

	logger.Log(googleEntry)

	return nil
}

func (g *googleCore) Sync() error {

	for _, logger := range g.loggers {

		err := logger.Flush()
		if err != nil {
			return err
		}
	}

	return g.output.Sync()
}

// https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry
func googleEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "severity",
		TimeKey:        "timestamp",
		NameKey:        "logName",
		CallerKey:      "caller",
		StacktraceKey:  "trace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.RFC3339TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}
