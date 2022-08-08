# ROS Bridge Output Plugin

This plugin writes telegraf metrics to ROS bridges

## Configuration

```toml @sample.conf
[[outputs.rosbridge_pub]]

## Bridge servers.
  bridges = ["ws://localhost:9000"]  

## Set timeout
  # timeout = "1s"

## set Max Queue size    
  queue_size = 100
 
```
