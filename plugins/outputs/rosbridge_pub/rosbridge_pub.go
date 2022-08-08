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

// ros bridge publisher client for telegraf
package rosbridge_pub

import (
	_ "embed"
	"errors"
	"log"
	"sync"
	"time"

	"encoding/json"
	"github.com/otamajakusi/roslibgo"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"

	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"	
)

// DO NOT REMOVE THE NEXT TWO LINES! This is required to embed the sampleConfig data.
//go:embed sample.conf
var sampleConfig string

type RosBridgePublisher struct {
	Bridges	               	[]string        			`toml:"bridges"`	
	ConnectTimeout	   	   	config.Duration 			`toml:"connect_timeout"`
	MaxQueueSize			int							`toml:"queue_size"`
	Log telegraf.Logger 				   				`toml:"-"`
	CliGroup				*pubCliGroup
	wg     sync.WaitGroup	
	serializer serializers.Serializer
}


func (m *RosBridgePublisher) SampleConfig() string {
	return sampleConfig
}

func (m *RosBridgePublisher) SetSerializer(serializer serializers.Serializer) {
	m.serializer = serializer	
}

func (m *RosBridgePublisher) Init() error {
	return nil
}


func (m *RosBridgePublisher) Connect() error {
	if len(m.Bridges)==0 {
		return errors.New("No bridge specified in config")
	}

	if m.MaxQueueSize == 0 {
		m.MaxQueueSize = 300
	}
	m.CliGroup = pubCliGroupNew(m.Bridges, m.MaxQueueSize)
	m.CliGroup.Run(&m.wg)

	return nil
}



func (m *RosBridgePublisher) Close() error {
	m.CliGroup.Close()
	m.wg.Wait()
	return nil
}

func (m *RosBridgePublisher) Write(metrics []telegraf.Metric) error {
	for _, metric := range metrics {
		m.CliGroup.AddPublish(metric)		
	}
	return nil
}


type pubCliGroup struct {
	pubClis		[]*pubCli
	prevTime 	time.Time
	Queue		chan telegraf.Metric
	EndQueue	chan struct{}
	index		int
}

type pubCli struct {
	url			string
	ros 		*roslibgo.Ros
	pubs 		map[string]*roslibgo.Topic
}

func (p *pubCli) Publish(topicName, topicType string ,jdata []byte) {
	pub := p.pubs[topicName]
	if pub == nil {
		pub = roslibgo.NewTopic(p.ros, topicName, topicType)
		p.pubs[topicName] = pub
	}

	err := pub.Publish(json.RawMessage(jdata))
	if err != nil {
		log.Fatal(err)
	}
}

func (p *pubCliGroup) AddPublish(metric telegraf.Metric) {	
	p.index = p.index + 1
	// log.Printf("AddPublish called %d\n", p.index)
	select {
		case <-p.EndQueue:
			return		
		case p.Queue <- metric:
			return
	}		
}

func (p *pubCliGroup) Run(wg *sync.WaitGroup) {	
	wg.Add(1)
	var inQueue []telegraf.Metric
	out:=make(chan telegraf.Metric)
	go func() {
		defer wg.Done()

		outCh := func() chan telegraf.Metric {
			if len(inQueue) == 0{
				return nil
			}
			return out
		}

		curVal := func() telegraf.Metric {
			if len(inQueue) == 0{
				return nil
			}
			return inQueue[0]
		}

		for {
			select {
				case v, ok := <-p.Queue:
					if !ok {						
						close(out)
						return
					} else {
						inQueue = append(inQueue, v)
					}
				case outCh()<-curVal():
					inQueue = inQueue[1:]

				case <-p.EndQueue:					
					close(out)
					return
			}
		}		
	}()
	wg.Add(1)	
	go func() {
		defer wg.Done()		
		for {
			select {
				case metric := <-out:
					if metric != nil {
						p.Publish(metric)
					}					
				case <-p.EndQueue:					
					return
			}
		}
	}()
	
}

func (p *pubCliGroup) Close() {
	close(p.EndQueue)
	close(p.Queue)	
}

func (p *pubCliGroup) Publish(metric telegraf.Metric) error {		
	topicName := metric.Name()
	topicType, _ := metric.GetTag("type")
	topicData, err := json.Marshal(metric.Fields())
	curTime := metric.Time()
	if p.prevTime.IsZero() {
		p.prevTime = curTime
	}

	sleepDuration := curTime.Sub(p.prevTime)	
	time.Sleep(sleepDuration)
	if err == nil {
		for _, cli := range p.pubClis {
			cli.Publish(topicName, topicType, topicData)		
		}
	}

	return nil
}

func pubCliGroupNew(bridges []string, queue_size int) *pubCliGroup {	
	clis := &pubCliGroup{		
		Queue: make(chan telegraf.Metric,queue_size),
		EndQueue: make(chan struct{}),
	}		

	var err error

	for _, url := range bridges {
		cli := &pubCli {}
		cli.url = url
		cli.ros, err = roslibgo.NewRos(url)
		if err != nil {
			log.Fatal(err)			
		}		
		cli.pubs = make(map[string]*roslibgo.Topic)
		cli.ros.Run()
		clis.pubClis = append(clis.pubClis, cli)
	}
	return clis
}


// Register the plugin
func init() {
	outputs.Add("rosbridge_pub", func() telegraf.Output {
		return &RosBridgePublisher{}
	})
}