FROM golang:1.23 AS builder
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod tidy

# Copy the rest of the source code
COPY . .

# Set the working directory to the fetcher command directory
WORKDIR /app/cmd/cleaner

# Build the Go binary
RUN go build -o cleaner-service main.go

FROM ubuntu:latest
WORKDIR /app/cmd/cleaner

# Install necessary libraries and CA certificates
RUN apt-get update && apt-get install -y libc6 ca-certificates

# Copy the built binary from the builder stage
COPY --from=builder /app/cmd/cleaner/cleaner-service .

# Ensure the binary is executable
RUN chmod +x cleaner-service

CMD ["./cleaner-service"]