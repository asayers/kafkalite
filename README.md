### Consume your Kafka streams without spinning up the JVM!

This package is designed to address the case where you have a bunch of
Kafka streams stored on a local disk, and want to consume them without
having to start a Kafka instance. It presents a read-only streaming
interface to your kafka streams, and handles efficient seeking and
decompression.

This package also contains an example program, which simply streams
kafka topics to stdout.

See [here] for documentation.

[here]: https://asayers.github.io/kafkar/Database-Kafkar.html
