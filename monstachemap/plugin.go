package monstachemap

// plugins must import this package
// import "github.com/rwynn/monstache/monstachemap"

// plugins must implement a function named "Map" with the following signature
// func Map(input *monstachemap.MapperPluginInput) (output *monstachemap.MapperPluginOutput, err error)

// plugins can be compiled using go build -buildmode=plugin -o myplugin.so myplugin.go
// to enable the plugin start with monstache -mapper-plugin-path /path/to/myplugin.so

// MapperPluginInput is the input to the Map function
type MapperPluginInput struct {
	Document   map[string]interface{} // the original document from MongoDB
	Database   string                 // the origin database in MongoDB
	Collection string                 // the origin collection in MongoDB
	Namespace  string                 // the entire namespace for the original document
	Operation  string                 // "i" for a insert or "u" for update
}

// MapperPluginOutput is the output of the Map function
type MapperPluginOutput struct {
	Document    map[string]interface{} // an updated document to index into Elasticsearch
	Index       string                 // the name of the index to use
	Type        string                 // the document type
	Routing     string                 // the routing value to use
	Drop        bool                   // set to true to indicate that the document should not be indexed
	Passthrough bool                   // set to true to indicate the original document should be indexed unchanged
}
