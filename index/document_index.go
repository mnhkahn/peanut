package index

// Param is search param.
type Param struct {
	PKs      []string
	Query    string
	Tags     []string
	Category string

	Offset int
	Size   int
}

// Document ...
type Document struct {
	DocId    uint32   `json:"-"`
	PK       string   `json:"pk"`
	Title    string   `json:"title"`
	PubDate  int64    `json:"pub_date"`
	Brief    string   `json:"brief"`
	FullText string   `json:"full_text"`
	Tags     []string `json:"tags"`
	Category string   `json:"category"`
	Link     string   `json:"link"`
	Figure   string   `json:"figure"`
}
