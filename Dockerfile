FROM rust:1.41
WORKDIR /usr/src/gtsa
COPY . .
RUN cargo install --path .

CMD ["gtsa"]