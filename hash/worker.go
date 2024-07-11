package hash

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)


func connectHashConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	fmt.Println("conn", conn)
	return conn, nil
}

func Hash()  {
	topicHash := "Hash"
	worker, err := connectHashConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}

	consumerHash, err := worker.ConsumePartition(topicHash , 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("hash consumer started")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	hashCount := 0
	// Get signal for finish
	doneCh := make(chan struct{})
	// msgChan := make(chan  *sarama.ConsumerMessage)
	go func() {
		for {
			select {
			case err := <-consumerHash.Errors():
				fmt.Println(err)
			case hash := <-consumerHash.Messages():
				hashCount++
				fmt.Printf("Received hash Count %d: | Topic(%s) | Message(%s) \n", hashCount, string(hash.Topic), string(hash.Value))
				// msgChan <- hash
			case <-sigchan:
				fmt.Println("Interrupt is detected")
			}
		}
	}()

	<- doneCh
	fmt.Println("Processed", hashCount, "hash")
	if err := worker.Close(); err != nil {
		panic(err)
	}
}

