package kafkasync

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/isi-nc/go-diff"
)

const indexBatchSize = 500

var Debug = false

type Syncer struct {
	// The topic to synchronize.
	Topic string

	// The topic's partition to synchronize.
	Partition int32

	// The value to use when a key is removed.
	RemovedValue []byte

	// Don't really send messages
	DryRun bool
}

func New(topic string) Syncer {
	return Syncer{
		Topic:        topic,
		Partition:    0,
		RemovedValue: []byte{},
	}
}

type KeyValue = diff.KeyValue

// Sync synchronize a key-indexed data source with a topic.
//
// The kvSource channel provides values in the reference store. It MUST NOT produce duplicate keys.
func (s Syncer) Sync(kafka sarama.Client, kvSource <-chan KeyValue, cancel <-chan bool) (stats *Stats, err error) {
	return s.SyncWithIndex(kafka, kvSource, diff.NewIndex(false), cancel)
}

// SyncWithIndex synchronize a data source with a topic, using the given index.
//
// The kvSource channel provides values in the reference store. It MUST NOT produce duplicate keys.
func (s Syncer) SyncWithIndex(kafka sarama.Client, kvSource <-chan KeyValue, topicIndex diff.Index, cancel <-chan bool) (stats *Stats, err error) {
	stats = NewStats()

	// Read the topic
	if Debug {
		log.Print("Reading topic ", s.Topic, ", partition ", s.Partition)
	}

	msgCount, err := s.IndexTopic(kafka, topicIndex)
	if err != nil {
		return
	}

	stats.MessagesInTopic = msgCount

	if Debug {
		log.Print("Read ", msgCount, " messages from topic.")
	}

	stats.ReadTopicDuration = stats.Elapsed()

	err = s.syncWithPrepopulatedIndex(kafka, kvSource, topicIndex, stats, cancel)
	return
}

func (s Syncer) SyncWithPrepopulatedIndex(kafka sarama.Client, kvSource <-chan KeyValue, topicIndex diff.Index, cancel <-chan bool) (stats *Stats, err error) {
	stats = NewStats()
	err = s.syncWithPrepopulatedIndex(kafka, kvSource, topicIndex, stats, cancel)
	return
}

func (s Syncer) syncWithPrepopulatedIndex(kafka sarama.Client, kvSource <-chan KeyValue, topicIndex diff.Index, stats *Stats, cancel <-chan bool) (err error) {
	defer func() {
		if err := topicIndex.Cleanup(); err != nil {
			if Debug {
				log.Printf("Cleanup error: %v", err)
			}
		}
	}()

	// Prepare producer
	send, finish := s.SetupProducer(kafka, stats)

	// Compare and send changes
	startSyncTime := time.Now()

	changes := make(chan diff.Change, 10)
	go func() {
		defer close(changes)
		if err = diff.DiffStreamIndex(kvSource, topicIndex, changes, cancel); err != nil {
			return
		}
		if Debug {
			log.Printf("Sync to %s partition %d finished", s.Topic, s.Partition)
		}
	}()

	s.ApplyChanges(changes, send, stats, cancel)
	finish()

	stats.SyncDuration = time.Since(startSyncTime)
	stats.TotalDuration = stats.Elapsed()

	return
}

func (s *Syncer) SetupProducer(kafka sarama.Client, stats *Stats) (send func(KeyValue), finish func()) {
	if s.DryRun {
		send = func(kv KeyValue) {
			if Debug {
				log.Printf("Would have sent: key=%q value=%q", string(kv.Key), string(kv.Value))
			}
		}
		finish = func() {}
		return
	}

	producer, err := sarama.NewAsyncProducerFromClient(kafka)
	if err != nil {
		return
	}

	wg := &sync.WaitGroup{}
	if kafka.Config().Producer.Return.Errors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for prodError := range producer.Errors() {
				log.Print("kafka-sync: got error from producer: ", prodError)
				stats.ErrorCount++
			}
		}()
	} else {
		stats.ErrorCount = -1
	}

	if kafka.Config().Producer.Return.Successes {
		wg.Add(1)
		go func() {
			for range producer.Successes() {
				stats.SuccessCount++
			}
			wg.Done()
		}()
	} else {
		stats.SuccessCount = -1
	}

	producerInput := producer.Input()

	send = func(kv KeyValue) {
		producerInput <- &sarama.ProducerMessage{
			Topic:     s.Topic,
			Partition: s.Partition,
			Key:       sarama.ByteEncoder(kv.Key),
			Value:     sarama.ByteEncoder(kv.Value),
		}
		stats.SendCount++
	}
	finish = func() {
		producer.AsyncClose()
		wg.Wait()
	}
	return
}

func (s *Syncer) ApplyChanges(changes <-chan diff.Change, send func(KeyValue), stats *Stats, cancel <-chan bool) {
	for {
		var (
			change diff.Change
			ok     bool
		)

		select {
		case <-cancel:
			return // cancelled

		case change, ok = <-changes:
			if !ok {
				// end of changes
				return
			}
		}

		switch change.Type {
		case diff.Deleted:
			send(KeyValue{change.Key, s.RemovedValue})

		case diff.Created, diff.Modified:
			send(KeyValue{change.Key, change.Value})
		}
		switch change.Type {
		case diff.Deleted:
			stats.Deleted += 1

		case diff.Unchanged:
			stats.Unchanged += 1
			stats.Count += 1

		case diff.Created:
			stats.Created += 1
			stats.Count += 1

		case diff.Modified:
			stats.Modified += 1
			stats.Count += 1
		}
	}
}

func (s *Syncer) IndexTopic(kafka sarama.Client, index diff.Index) (msgCount uint64, err error) {
	if Debug {
		log.Printf("IndexTopic: starting")
		defer log.Printf("IndexTopic: finished")
	}

	topic := s.Topic
	partition := s.Partition

	lowWater, err := kafka.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return
	}
	highWater, err := kafka.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return
	}

	if Debug {
		log.Printf("-> low/high water: %d/%d", lowWater, highWater)
	}

	if highWater == 0 || lowWater == highWater {
		// topic is empty
		return
	}

	resumeKey, err := index.ResumeKey()
	if err != nil {
		return
	}

	var resumeOffset int64
	if resumeKey == nil {
		if Debug {
			log.Print("-> no resume information, starting from oldest")
		}
		resumeOffset = sarama.OffsetOldest
	} else {
		_, err = fmt.Fscanf(bytes.NewBuffer(resumeKey), "%x", &resumeOffset)
		if err != nil {
			return
		}

		resumeOffset++

		if Debug {
			log.Print("-> resume offset: ", resumeOffset)
		}

		if resumeOffset >= highWater {
			if Debug {
				log.Printf("-> would consume from %d, high water is %d, so we're up-to-date",
					resumeOffset, highWater)
			}
			return
		}
	}

	consumer, err := sarama.NewConsumerFromClient(kafka)
	if err != nil {
		return
	}

	pc, err := consumer.ConsumePartition(topic, partition, resumeOffset)
	if err != nil {
		return
	}

	wg := &sync.WaitGroup{}
	resumeOffset = -1

	kvs := make(chan KeyValue, indexBatchSize)
	resumeKeyCh := make(chan []byte, 1)

	doIndex := func() {
		defer wg.Done()

		err := index.Index(kvs, resumeKeyCh)
		if err != nil {
			panic(err) // FIXME
		}
	}

	// start indexing
	wg.Add(1)
	go doIndex()

	saveBatch := func(restart bool) {
		// finalize indexing
		if Debug {
			log.Print("finalize")
		}

		close(kvs)
		resumeKeyCh <- []byte(fmt.Sprintf("%16x", resumeOffset))
		wg.Wait()

		if restart {
			// start next indexing
			kvs = make(chan KeyValue, indexBatchSize)
			resumeKeyCh = make(chan []byte, 1)
			wg.Add(1)
			go doIndex()
		}
	}

	// report kafka errors
	go func() {
		// FIXME fail on first error
		for m := range pc.Errors() {
			if Debug {
				log.Printf("-> got kafka error %+v", m)
			}
		}
	}()

	timer := time.NewTimer(kafka.Config().Consumer.MaxProcessingTime)
	defer timer.Stop()
	msgCount = 0
consume:
	for {
		select {
		case m := <-pc.Messages():
			{
				hw := pc.HighWaterMarkOffset()
				if hw > highWater {
					highWater = hw
				}

				if Debug {
					log.Print("-> offset: ", m.Offset, " / ", highWater-1)
				}

				value := m.Value
				if bytes.Equal(value, s.RemovedValue) {
					value = nil
				}
				kvs <- KeyValue{m.Key, value}
				msgCount++

				resumeOffset = m.Offset

				if m.Offset+1 >= highWater {
					break consume
				}

				if msgCount%indexBatchSize == 0 {
					saveBatch(true)
				}
			}
			// timeout if unable to read messages from kafka for a while
		case <-timer.C:
			pc.Close()
			consumer.Close()
			return msgCount, errors.New("timed out while waiting for kafka message")
		}
		timer.Reset(kafka.Config().Consumer.MaxProcessingTime)
	}

	pc.Close()
	consumer.Close()

	saveBatch(false)

	return
}
