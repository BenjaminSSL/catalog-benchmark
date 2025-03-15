package common

import (
	"github.com/schollz/progressbar/v3"
	"log"
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
		bufferSize: uint64(20),
	}
}

func (p *ProgressBar) SetBufferSize(size uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bufferSize = size
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
	err := p.bar.Add(int(val))
	if err != nil {
		log.Printf("Error saving buffer: %s\n", err)
		return
	}
	p.mu.Unlock()
}
