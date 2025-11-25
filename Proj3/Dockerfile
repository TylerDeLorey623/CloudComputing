# -- BUILD STAGE --
FROM golang:1.25.1-alpine AS build-stage

# Install build dependencies
RUN apk add --no-cache upx

WORKDIR /app

# Download and Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY *.go .

# Build static binary with stripped debug info
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o proj3

# Compress binary with UPX
RUN upx --best --lzma proj3

# -- RUNTIME STAGE --
FROM scratch

WORKDIR /app

# Copy binary from builder stage
COPY --from=build-stage /app/proj3 .

# Copy all input files to the image
COPY *.txt .

# Run default command
CMD ["./proj3"]