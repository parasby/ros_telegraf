# ROS Bridge Input Plugin

This plugin subscribes ROS messages from ROS bridges

## Configuration

```toml @sample.conf
[[inputs.rosbridge_sub]]

## Bridge servers.
  bridges = ["ws://localhost:9000"]

  ## Topics to consume.
  topics = {"TOPIC_NAME" = "TOPIC_TYPE","TOPIC_NAME2" = "TOPIC_TYPE2" }

  ## Set timeout
  # timeout = "1s"
```
