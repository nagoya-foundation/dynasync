package main

type Commit struct {
	Date    int64    `json:"date"`
	File    string   `json:"filePath"`
	Hash    [16]byte `json:"hash"`
	Diff    string   `json:"diff"`
	Message string   `json:"message"`
}

type Tag struct {
	Date int64  `json:"date"`
	File string `json:"filePath"`
	Text string `json:"tag"`
}

