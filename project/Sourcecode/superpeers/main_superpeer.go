// main_superpeer.go
package main

import (
    "flag"
    "io/ioutil"
    "log"
    "sync"
    "encoding/json"
)

// loadSuperPeerConfig loads the SuperPeer configuration from a JSON file
func loadSuperPeerConfig(filename string) (SuperPeerConfig, error) {
    data, err := ioutil.ReadFile(filename)
    if err != nil {
        return SuperPeerConfig{}, err
    }
    var config SuperPeerConfig
    err = json.Unmarshal(data, &config)
    return config, err
}


// main function to run the SuperPeer
func main() {
    configPath := flag.String("config", "superpeer_config.json", "Path to SuperPeer configuration JSON file")
    flag.Parse()

    // Read configuration file
    configData, err := ioutil.ReadFile(*configPath)
    if err != nil {
        log.Fatalf("SuperPeer: Failed to read config file '%s': %v", *configPath, err)
    }

    var config SuperPeerConfig
    err = json.Unmarshal(configData, &config)
    if err != nil {
        log.Fatalf("SuperPeer: Failed to parse config file '%s': %v", *configPath, err)
    }

    // Initialize SuperPeer
    superPeer := NewSuperPeer(config)

    // Start SuperPeer
    var wg sync.WaitGroup
    wg.Add(1)
    go superPeer.Start(&wg)
    wg.Wait()
}

