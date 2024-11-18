// main_leafnode.go
package main

import (
    "encoding/json"
    "flag"
    "io/ioutil"
    "log"
    "sync"
)

// loadLeafNodeConfig loads the LeafNode configuration from a JSON file
func loadLeafNodeConfig(filename string) (LeafNodeConfig, error) {
    var config LeafNodeConfig
    data, err := ioutil.ReadFile(filename)
    if err != nil {
        return config, err
    }
    err = json.Unmarshal(data, &config)
    return config, err
}

func main() {
    // Parse command-line arguments for config file
    configFile := flag.String("config", "", "Path to LeafNode config JSON file")
    flag.Parse()

    // Check if the config file is provided
    if *configFile == "" {
        log.Println("Usage: leafnode -config=<config_file>")
        return
    }

    // Load LeafNode configuration
    config, err := loadLeafNodeConfig(*configFile)
    if err != nil {
        log.Fatalf("Failed to load LeafNode config: %v", err)
    }

    // Initialize LeafNode
    leafNode := NewLeafNode(config)

    // Start the LeafNode client
    var wg sync.WaitGroup
    wg.Add(1)
    go leafNode.Start(&wg)

    // Keep the main goroutine alive
    wg.Wait()
}
