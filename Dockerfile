FROM clux/muslrust:stable as builder
RUN apt-get update && apt-get -y install ca-certificates libssl-dev
WORKDIR /usr/src/gtsa
COPY . .
RUN cargo build --release

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /usr/src/gtsa/target/x86_64-unknown-linux-musl/release/gtsa .
CMD ["./gtsa"]