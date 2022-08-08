//go:generate ../../../tools/readme_config_includer/generator

// MIT License
// 
// Copyright (c) 2022, Electronics and Telecommunications Research Institute (ETRI) All Rights Reserved.
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// rosbridge_sub.go (ros bridge plugin)
// Author: paraby@gmail.com


// ros bridge subscriber client for telegraf
package rosbridge_sub

import (
	_ "embed"	
	"log"
	"sync"
	"encoding/json"
	"github.com/otamajakusi/roslibgo"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
)

// DO NOT REMOVE THE NEXT TWO LINES! This is required to embed the sampleConfig data.
//go:embed sample.conf
var sampleConfig string

type empty struct{}
type semaphore chan empty

type RosBridgeSubscriber struct {
	Bridges	               	[]string        			`toml:"bridges"`
	Topics                 	map[string]string  			`toml:"topics"`
	ConnectTimeout	   	   	config.Duration 			`toml:"connect_timeout"`
	Log telegraf.Logger 				   				`toml:"-"`	

	parser parsers.Parser
	wg     sync.WaitGroup	
}


func (m *RosBridgeSubscriber) SampleConfig() string {
	return sampleConfig
}

func (m *RosBridgeSubscriber) SetParser(parser parsers.Parser) {
	m.parser = parser
}

func (m *RosBridgeSubscriber) Init() error {
	return nil
}


func (m *RosBridgeSubscriber) Start(acc telegraf.Accumulator) error {
	for _, bridge := range m.Bridges {
		m.wg.Add(1)
		go func(server string) {
			defer m.wg.Done()
			acc.AddError(m.gatherServer(acc, server))
		}(bridge)
	}

	return nil
}

func (m *RosBridgeSubscriber) Gather(_ telegraf.Accumulator) error {
	return nil
}

func (m *RosBridgeSubscriber) Stop() {
	m.wg.Wait()
}


func (m *RosBridgeSubscriber) gatherServer(
	acc telegraf.Accumulator, 	serverURL string,
) error {
	ros, _ := roslibgo.NewRos(serverURL)
	ros.Run()

	for topicName, topicType := range m.Topics {
		sub := roslibgo.NewTopic(ros, topicName, topicType)
		sub.Subscribe(func(data json.RawMessage){
			fields := make(map[string]interface{})
			err := json.Unmarshal(data, &fields)
			if err == nil {
				tags := map[string]string{"url" : serverURL, "type" : topicType}
				acc.AddFields(topicName,fields, tags)
			} else {
				log.Printf("Unmarshal error %v\n",err)
			}			
		})
	}
	return nil
}
// Register the plugin
func init() {
	inputs.Add("rosbridge_sub", func() telegraf.Input {
		return &RosBridgeSubscriber{}
	})
}