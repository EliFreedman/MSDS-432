FROM golang:1.23 AS builder
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod tidy

# Copy the rest of the source code
COPY . .

# Set the working directory to the fetcher command directory
WORKDIR /app/cmd/transformer

# Build the Go binary
RUN go build -o transformer-service main.go

FROM ubuntu:latest
WORKDIR /app/cmd/transformer

# Install necessary libraries and CA certificates
RUN apt-get update && apt-get install -y libc6 ca-certificates

# Copy the built binary from the builder stage
COPY --from=builder /app/cmd/transformer/transformer-service .

# Ensure the binary is executable
RUN chmod +x transformer-service

CMD ["./transformer-service"]