package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type durableLog struct {
	file *os.File
}

func createLog(path string) (*durableLog, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, errors.Join(err, fd.Close())
	}
	if err := errors.Join(fd.Sync(), parent.Sync(), parent.Close()); err != nil {
		return nil, errors.Join(err, fd.Close())
	}
	return &durableLog{file: fd}, nil
}

func (log *durableLog) record(value any) error {
	if err := json.NewEncoder(log.file).Encode(value); err != nil {
		return err
	}
	return log.file.Sync()
}

func (log *durableLog) close() error {
	return errors.Join(log.file.Sync(), log.file.Close())
}

func writeInput(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, writeErr := fd.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = errors.New("short qualification input write")
	}
	return errors.Join(writeErr, fd.Sync(), fd.Close())
}

func replaceInputLink(source, target string) error {
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Link(source, target)
}
