package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct{
	Text string `form:"text" json:"text"`
}

func main(){
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	api.Post("/hash", SendHash)
	app.Listen(":3000")
}

func ConnectProducer(brokersUrl []string)(sarama.SyncProducer, error){
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushMessageToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	fmt.Println("topic",topic)

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func createComment(c *fiber.Ctx) error {

	// Instantiate new Message struct
	cmt := new(Comment)

	//  Parse body into comment struct
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	// convert body into bytes and send it to kafka
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		fmt.Println("failed to marshal jason data ")
	}
	err = PushMessageToQueue("comments", cmtInBytes)
	if err != nil {
		fmt.Println("failed to push comments to queue")
	}

	// Return Comment in JSON format
	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return err
}

type Hash struct{
	Hash string `form:"text" json:"text"`
}

func SendHash(c *fiber.Ctx) error {

	// Instantiate new Message struct
	hash := new(Hash)

	//  Parse body into Hash struct
	if err := c.BodyParser(hash); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	// convert body into bytes and send it to kafka
	hashInBytes, err := json.Marshal(hash)
	if err != nil {
		fmt.Println("failed to marshal jason data ")
	}
	err = PushhashToQueue("Hash", hashInBytes)
	fmt.Println("hash in byte", hashInBytes)

	if err != nil {
		fmt.Println("failed to push Hashs to queue")
	}

	// Return Hash in JSON format
	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Hash pushed successfully",
		"Hash": hash,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return err
}

func PushhashToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	fmt.Println("topic",topic)

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}