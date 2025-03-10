package common

import (
	"github.com/schollz/progressbar/v3"
	"sync"
	"sync/atomic"
)

type ProgressBar struct {
	bar        *progressbar.ProgressBar
	buffer     atomic.Uint64
	bufferSize uint64
	mu         sync.Mutex
}

func NewProgressBar(total int) *ProgressBar {
	return &ProgressBar{
		bar: progressbar.NewOptions(total,
			progressbar.OptionEnableColorCodes(true),
			progressbar.OptionShowCount(),
			progressbar.OptionSetWidth(50),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "[green]=[reset]",
				SaucerHead:    "[green]>[reset]",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
			progressbar.OptionClearOnFinish(),
			progressbar.OptionShowIts(),
			progressbar.OptionSetItsString("request"),
		),
		bufferSize: uint64(100),
	}
}

func (p *ProgressBar) Add(n int) {
	// Add to buffer and flush if limit is reached
	if p.buffer.Add(uint64(n)) >= p.bufferSize {
		p.Flush()
	}
}

func (p *ProgressBar) Flush() {
	p.mu.Lock()
	val := p.buffer.Swap(0)
	p.bar.Add(int(val))
	p.mu.Unlock()
}
