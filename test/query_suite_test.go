package test

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/Shopify/sarama"
	cmodel "github.com/TerrexTech/go-common-models/model"

	"github.com/TerrexTech/go-kafkautils/kafka"

	"github.com/TerrexTech/rm-userauth-users/model"
	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/rm-userauth-users/connutil"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestQuery tests Query-handling.
func TestQuery(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"SERVICE_NAME",

		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_TOPIC_REQUEST",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",

		"MONGO_DATABASE",
		"MONGO_COLLECTION",
		"MONGO_CONNECTION_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "QueryHandler Suite")
}

var _ = Describe("UsersReadModel", func() {
	var (
		coll *mongo.Collection

		reqTopic  string
		respTopic string

		prodInput  chan<- *sarama.ProducerMessage
		consConfig *kafka.ConsumerConfig
	)

	BeforeSuite(func() {
		var err error
		coll, err = connutil.LoadMongoConfig()
		Expect(err).ToNot(HaveOccurred())

		reqTopic = os.Getenv("KAFKA_CONSUMER_TOPIC_REQUEST")
		respTopic = "test.response"

		kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
		kafkaBrokers := *commonutil.ParseHosts(kafkaBrokersStr)
		prod, err := kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		})
		prodInput = prod.Input()

		consConfig = &kafka.ConsumerConfig{
			KafkaBrokers: kafkaBrokers,
			Topics:       []string{respTopic},
			GroupName:    "test.group.1",
		}
	})

	It("should timeout query if it exceeds TTL", func(done Done) {
		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		mockCmd := cmodel.Command{
			Action:        "LoginUser",
			CorrelationID: cid,
			Data:          []byte("{}"),
			ResponseTopic: respTopic,
			Source:        "test-source",
			SourceTopic:   reqTopic,
			Timestamp:     time.Now().Add(-15 * time.Second).UTC().Unix(),
			TTLSec:        15,
			UUID:          uuid,
		}
		marshalCmd, err := json.Marshal(mockCmd)
		Expect(err).ToNot(HaveOccurred())

		msg := kafka.CreateMessage(reqTopic, marshalCmd)
		prodInput <- msg

		consumer, err := kafka.NewConsumer(consConfig)
		Expect(err).ToNot(HaveOccurred())

		success := true
		msgCallback := func(msg *sarama.ConsumerMessage) bool {
			defer GinkgoRecover()
			doc := &cmodel.Document{}
			err := json.Unmarshal(msg.Value, doc)
			Expect(err).ToNot(HaveOccurred())

			if doc.CorrelationID == mockCmd.UUID {
				success = false
				return true
			}
			return false
		}

		timeout := time.Duration(10) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		handler := &msgHandler{msgCallback}
		err = consumer.Consume(ctx, handler)
		Expect(err).ToNot(HaveOccurred())

		err = consumer.Close()
		Expect(err).To(HaveOccurred())
		Expect(success).To(BeTrue())
		close(done)
	}, 15)

	Describe("LoginUser", func() {
		It("should authenticate user-credentials", func(done Done) {
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			pswd := "test-password"
			mockUser := &model.User{
				UserID:    uid.String(),
				Email:     "test-email",
				FirstName: "test-fname",
				LastName:  "test-lname",
				UserName:  uid.String(),
				Role:      "test-role",
			}

			hashPass, err := bcrypt.GenerateFromPassword([]byte(pswd), 10)
			Expect(err).ToNot(HaveOccurred())
			mockUser.Password = string(hashPass)
			_, err = coll.InsertOne(mockUser)
			Expect(err).ToNot(HaveOccurred())

			cmdData, err := json.Marshal(&model.User{
				UserName: mockUser.UserName,
				Password: pswd,
			})
			Expect(err).ToNot(HaveOccurred())

			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockCmd := cmodel.Command{
				Action:        "LoginUser",
				CorrelationID: cid,
				Data:          cmdData,
				ResponseTopic: respTopic,
				Source:        "test-source",
				SourceTopic:   reqTopic,
				Timestamp:     time.Now().UTC().Unix(),
				TTLSec:        15,
				UUID:          uuid,
			}
			marshalCmd, err := json.Marshal(mockCmd)
			Expect(err).ToNot(HaveOccurred())

			msg := kafka.CreateMessage(reqTopic, marshalCmd)
			prodInput <- msg

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				doc := &cmodel.Document{}
				err := json.Unmarshal(msg.Value, doc)
				Expect(err).ToNot(HaveOccurred())

				if doc.CorrelationID == mockCmd.UUID {
					Expect(doc.Error).To(BeEmpty())
					Expect(doc.ErrorCode).To(BeZero())

					user := &model.User{}
					err = json.Unmarshal(doc.Data, user)
					Expect(err).ToNot(HaveOccurred())

					if user.UserID == mockUser.UserID {
						user.Password = ""
						mockUser.Password = ""
						Expect(mockUser).To(Equal(mockUser))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 10)
	})
})
