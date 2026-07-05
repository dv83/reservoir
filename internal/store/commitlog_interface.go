package store

// CommitLogger is an interface for commit log to avoid import cycles
type CommitLogger interface {
	Write(operation string, key []byte, value []byte) error
	Delete(key []byte)
	Close() error
}