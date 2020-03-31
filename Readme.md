# GTSA project
[![](https://docs.rs/gtsa/badge.svg)](https://docs.rs/gtsa/)
![Rust](https://github.com/Mnwa/gtsa/workflows/Build/badge.svg?branch=master)
[![](https://img.shields.io/crates/v/gtsa.svg)](https://crates.io/crates/gtsa)
[![](https://img.shields.io/crates/d/gtsa.svg)](https://crates.io/crates/gtsa)

Gelf To Sentry Adapter is the simple solution to proxy gelf messages (messages for [Graylog](https://www.graylog.org/)) to [Sentry](https://sentry.io/)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

You need to docker or cargo with rust

### Installing and running

You can install and run it with cargo 

```bash
cargo install gtsa
gtsa
```

Docker hub
```bash
docker pull mnwamnowich/gtsa
docker run -p 8080:8080/udp --env SENTRY_DSN=dsn --name gtsa mnwamnowich/gtsa
```

Or docker build

```bash
docker build -t gtsa .
docker run -p 8080:8080/udp --env SENTRY_DSN=dsn --name gtsa gtsa 
```

Now you can sent data on udp 

## Deployment

For deploying GTSA you must to set is only one env variable:
```env
SENTRY_DSN=xxx
```

SENTRY DSN must be only in the new format (without secret key)!

Also GTSA have a some env variables with default values, but you can edit it:
```env
UDP_ADDR=0.0.0.0:8080 //udp address
TCP_ADDR=0.0.0.0:8081 // tcp address
SYSTEM=Gelf Mover // name of your application
READER_THREADS=1 // threads for decoding and encoding json messages (max parrallel messages processing)
UNPACKER_THREADS=1 // threads for unpacking messages what received with gz or zlib algoritms (max parrallel messages unpacking)
MAX_PARALLEL_CHUNKS=100000 // maximum chunked messages what GTSA can processing, old messages will be flush (udp only)
```

## Examples

### Sending udp message
```bash
 echo -n -e '{ "version": "1.1", "host": "example.org", "short_message": "A short message", "level": 5, "_some_info": "foo", "timestamp": 1582213226}'"" | nc -w0 -u 0.0.0.0 8080
```

### Sending tcp message
```bash
echo -n -e '{ "version": "1.1", "host": "example.org", "short_message": "A short message", "level": 5, "_some_info": "foo", "timestamp": 1582213226}'"\0" | nc -w0 0.0.0.0 8081
```

## Built With

* [actix](https://github.com/actix/actix) - The actor framework used
* [tokio](https://github.com/tokio-rs/tokio) - Async realisation
* [cargo](https://github.com/rust-lang/cargo) - Built with

## Contributing

I hope you know what you need to do.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/Mnwa/gtsa/tags). 

## Authors

* **Mikhail Panfilov** - *Initial work* - [Mnwa](https://github.com/Mnwa)

See also the list of [contributors](https://github.com/Mnwa/gtsa/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
