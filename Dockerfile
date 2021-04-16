FROM golang:1.13.1-alpine AS builder

WORKDIR /home/taichong/GoPath/src/github.com/TCeason/tsbs
WORKDIR /tsbs
COPY ./ ./
RUN apk update && apk add --no-cache git
ENV GOPROXY=https://goproxy.cn,direct
RUN go mod download && go install ./...

# Final stage
FROM alpine:3.8.5
RUN apk update && apk add --no-cache bash
COPY --from=builder /go/bin /bin
COPY --from=builder /tsbs/scripts /bin
# We need to keep the container running since there is no background process
ENTRYPOINT ["/bin/bash", "-c", "trap : TERM INT; (while true; do sleep 1000; done) & wait"]

