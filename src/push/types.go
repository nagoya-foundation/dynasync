package main

type Commit struct {
	Id      []byte `json:"commitId"`
	Date    string `json:"date"`
	Diff    string `json:"diff"`
	Message string `json:"message"`
}

