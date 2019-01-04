package main

type Commit struct {
	Date    int64  `dynamodbav:"commitDate"`
	File    string `dynamodbav:"filePath"`
	Author  string `dynamodbav:"author"`
	Diff    string `dynamodbav:"diff"`
	Message string `dynamobdav:"message"`
}

type RepoIndex struct {
	Repo         string   `dynamodbav:"repo"`
	Files        []string `dynamodbav:"files,stringset,omitempty"`
	Commits      []int64  `dynamodbav:"commits,numberset,omitempty"`
	CreationDate int64    `dynamodbav:"creationDate"`
	Owner        string   `dynamodbav:"owner"`
}

type Tag struct {
//	Date int64  `json:"date"`
	File string `json:"filePath"`
	Text string `json:"tag"`
}

