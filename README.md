# go-kafka-producer

## 1resilientkafkaconsumer.go

Designed to show how it is possible to supervise a kafka connection so that it get's rescheduled upon failure.

The other interesting aspect of it is that the producer runs on a different routine than the kafka connection.

