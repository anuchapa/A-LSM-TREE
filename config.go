package myLSMTree

import "time"

const (
	DefaultRootPath        = "./db_table/"
	DefaultMaxL0Files      = 4
	DefaultLevelMultiplier = 10
	DefaultMaxLevel        = 7
	DefaultSSTableSize     = 1024 * 1024
	IndexBlockSize         = 4 * 1024
	DefaultCompactPeriod   = 100 * time.Millisecond
)

type Configuration struct {
	RootPath           string
	MaxL0Files         int
	LevelMultiplier    int
	MaxLevel           int
	SSTableSize        int
	IndexBlockSize     int
	CompactCheckPeriod time.Duration
}

func DefaultConfiguration() Configuration {
	return Configuration{
		RootPath:           DefaultRootPath,
		MaxL0Files:         DefaultMaxL0Files,
		LevelMultiplier:    DefaultLevelMultiplier,
		MaxLevel:           DefaultMaxLevel,
		SSTableSize:        DefaultSSTableSize,
		CompactCheckPeriod: DefaultCompactPeriod,
	}
}
