package main

type Commit struct {
	File    string   `json:"filePath"`
	Date    int64    `json:"date"`
	Diff    string   `json:"diff"`
	Hash    [16]byte `json:"hash"`
	Message string   `json:"message"`
}

