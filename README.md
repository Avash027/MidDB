
<h1 align="center"> MidDB </h1>




<img src="./assets/icon.gif" height="100px" width="100px" align="left" style="margin-right:2rem"></img> A High-Performance and Fault-Tolerant Key-Value Store Built with LSM Trees and Bloom Filters with Strong Consistency Guarantees

<br />

### How it works?

- The server listens for TCP and UDP requests on the specified ports.
- The server uses a write-ahead log to store all the writes.
  - If the server crashes, the write-ahead log is used to recover the data.
  - The data is written to a buffer. If the buffer is full, the data is flushed to WAL.
  - In case of a crash, we get a signal from the OS. The server then flushes the buffer to WAL, thus ensuring that no data is lost.
  - Before the server starts, it checks if there is a write-ahead log file. If there is, it recovers the data from the write-ahead log.
  - The contents of write-ahead log are persisted to disk periodically.
- The server first checks in the binary search trees for the key. If the key is found, the value is returned. Otherwise, the server checks in the bloom filter. If the key is not found in the bloom filter, the server returns an error. If the key is found in the bloom filter, the server checks in the diskblocks. If the key is found in the diskblocks, the value is returned. Otherwise, the server returns an error.
    - A bloom filter is a probabilistic data structure that is used to test whether an element is a member of a set. False positives are possible, but false negatives are not. The bloom filter is used to reduce the number of sequential reads.
    - The diskblocks are stored on disk. Each diskblock contains a binary search tree. The diskblocks are merged periodically to reduce the number of disk seeks.



### Setup
- Clone the repository.
- Install Go.
- Run `go mod download` to download the dependencies.
- Copy the config.example.yaml file to config.yaml and modify the parameters as needed.
- Run `go run main.go -config={path_to_config_file}` to start the server.

### Configuration

The configuration file is in YAML format. The following parameters can be configured:

- `port` : The port on which the server will listen for requests. (Default: 8080)
- `host` :  The hostname or IP address to bind to. (Default: localhost)
- `max_elements_before_flush`: The maximum number of elements to store in memory before flushing to disk. (Default: 1024)
- `compaction_frequency_in_ms`: The frequency at which two diskblocks are merged. (Default: 1000)
- `wal_path`: The path to the write-ahead log file. (Default: wal.aof)
- `udp_port`: The UDP port number to listen on. (Default: 1053)
- `udp_buffer_size`: The size of the UDP buffer. (Default: 1024)
- `num_of_partitions`: The number of partitions to use. (Default: 10)
- `directory`: The directory where data files will be stored. (Default: data)
- `bloom_capacity`: The capacity of the bloom filter. (Default: 1000000)
- `bloom_error_rate`: The desired error rate for the bloom filter. (Default: 0.0001)


### Using TELNET to send requests

You can use Telnet to send TCP requests to a server. Here's how to do it

- Open the Terminal.

- Type telnet {host} {port} and press Enter. Replace {host} with the hostname or IP address of the server and {port} with the port number.

- Type the request and press Enter.

- Press Ctrl + ] to enter Telnet command mode.

- Type quit and press Enter to exit Telnet.

**Note: Telnet is not installed by default on some operating systems. You may need to install it manually.**


### Commands

- `SET key value` - Set the value of a key.
- `GET key` - Get the value of a key.
- `DEL key` - Delete a key.

#### Data types supported

- Strings


#### References

- LSM Trees: [Medium Article](https://eileen-code4fun.medium.com/log-structured-merge-tree-lsm-tree-implementations-a-demo-and-leveldb-d5e028257330)

- Bloom Filters: [DiceDB (Arpit Bhayani)](https://github.com/DiceDB/dice)

