FROM golang:1.23 AS builder
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod tidy

# Copy the rest of the source code
COPY . .

# Set the working directory to the fetcher command directory
WORKDIR /app/cmd/fetcher

# Build the Go binary
RUN go build -o fetcher-service main.go

# Debugging step: List the contents of the working directory after building
RUN echo "Contents of /app/cmd/fetcher after building:" && ls -la /app/cmd/fetcher

FROM ubuntu:latest
WORKDIR /app/cmd/fetcher

# Install necessary libraries and CA certificates
RUN apt-get update && apt-get install -y libc6 ca-certificates

# Copy the built binary from the builder stage
COPY --from=builder /app/cmd/fetcher/fetcher-service .

# Ensure the binary is executable
RUN chmod +x fetcher-service

# Debugging step: List the contents of the working directory in the final image
RUN echo "Contents of /app/cmd/fetcher in the final image:" && ls -la /app/cmd/fetcher

CMD ["./fetcher-service"]