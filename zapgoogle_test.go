package zapgoogle

import (
	"testing"
)

func TestGoogleEncoderConfig(t *testing.T) {
	cfg := googleEncoderConfig()
	if cfg.MessageKey != "message" {
		t.Errorf("expected MessageKey to be 'message', got %s", cfg.MessageKey)
	}
}

// Since we can't easily mock logging.Client without changing NewCore,
// we'll at least test the mapping logic if we can.
// For now, let's just verify the file compiles and the config is sane.
