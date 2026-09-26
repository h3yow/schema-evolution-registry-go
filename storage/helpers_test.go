package storage

import "os"

func writeTestFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

func readTestFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
