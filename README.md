<h1 align="center">
  go-retry
</h1>

<h4 align="center">Robust retry mechanism for Go applications, based on the AWS SDK's retry throttling mechanism.</h4>

<p align="center">
  <a href="#installation">Installation</a> •
  <a href="#usage">Usage</a> •
  <a href="#contributing">Contributing</a> •
  <a href="#license">License</a>
</p>

`go-retry` provides a robust retry mechanism for Go applications, enhancing resilience and reliability by automatically
retrying operations in the face of transient failures. Inspired by the AWS SDK's retry throttling mechanism, `go-retry`
throttles retries if too many retries fail.

## Installation

```bash
go get -u github.com/asimihsane/go-retry
```

## Usage

```go
package main

import (
	"context"

	"github.com/asimihsan/go-retry/retry"
)

func main() {
	retryer := retry.NewRetryer(retry.NewDefaultConfig())

	ctx := context.Background()

	err := retryer.Do(ctx, func(ctx context.Context) error {
		// Your code here
		return nil
	})
	if err != nil {
		// Handle error
	}
}
```

`go-retry` is easily configurable to suit a variety of retry strategies. Below are examples showcasing basic usage and
some idiomatic configurations including the optional circuit breaker mode, which is disabled by default.

```go
package main

import (
	"context"

	"github.com/asimihsan/go-retry/retry"
)

func main() {
	retryer := retry.NewRetryer(retry.NewDefaultConfig(),
		retry.WithRateLimitFirstRequestEnabled(),
	)

	ctx := context.Background()

	err := retryer.Do(ctx, func(ctx context.Context) error {
		// Your code here
		return nil
	})
	if err != nil {
		// Handle error
	}
}
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING](CONTRIBUTING.md) for details.

## License

`go-retry` is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full license text.
