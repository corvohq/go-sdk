# Corvo Go SDK

Go client and worker runtime for [Corvo](https://corvo.dev), the fast job queue.

## Installation

```bash
go get github.com/corvohq/go-sdk
```

## Compatibility

| SDK Version | Corvo Server |
|-------------|-------------|
| 0.2.x       | >= 0.2.0    |

## Quick Start

### Client (REST HTTP)

```go
package main

import (
    "fmt"
    "github.com/corvohq/go-sdk/client"
)

func main() {
    c := client.NewWithOptions("http://localhost:7080", client.WithAPIKey("your-key"))

    result, err := c.Enqueue("emails", map[string]string{"to": "user@example.com"})
    if err != nil {
        panic(err)
    }
    fmt.Println("Enqueued:", result.JobID)
}
```

### Worker

```go
package main

import (
    "context"
    "fmt"
    "github.com/corvohq/go-sdk/client"
)

func main() {
    w := client.NewWorker(client.WorkerConfig{
        URL:         "http://localhost:7080",
        Queues:      []string{"emails"},
        WorkerID:    "worker-1",
        Concurrency: 5,
    })

    w.Register("emails", func(job client.FetchedJob, ctx *client.JobContext) error {
        fmt.Printf("Processing job %s\n", job.JobID)
        return nil
    })

    w.Start(context.Background())
}
```

### RPC Client (Connect RPC)

```go
package main

import (
    "context"
    "fmt"
    "github.com/corvohq/go-sdk/rpc"
)

func main() {
    c := rpc.New("http://localhost:7080", rpc.WithAPIKey("your-key"))

    resp, err := c.Fetch(context.Background(), rpc.FetchRequest{
        Queues:   []string{"emails"},
        WorkerID: "worker-1",
        Hostname: "my-host",
    })
    if err != nil {
        panic(err)
    }
    if resp != nil {
        fmt.Println("Got job:", resp.JobID)
    }
}
```

## Authentication

- `client.WithBearerToken("token")` -- static bearer token
- `client.WithAPIKey("key")` -- API key via `X-API-Key` header
- `client.WithAPIKeyHeader("Custom-Header", "key")` -- API key via custom header
- `client.WithTokenProvider(func(ctx) (string, error))` -- dynamic token
- `client.WithHeader("key", "value")` -- arbitrary headers

## License

MIT
