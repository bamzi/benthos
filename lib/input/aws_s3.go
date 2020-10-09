package input

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func init() {
	Constructors[TypeAWSS3] = TypeSpec{
		constructor: func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := newAmazonS3(conf.AWSS3, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(
				TypeAWSS3,
				true,
				reader.NewAsyncBundleUnacks(
					reader.NewAsyncPreserver(r),
				),
				log, stats,
			)
		},
		Beta: true,
		Summary: `
Downloads objects within an S3 bucket, optionally filtered by a prefix. If an SQS queue has been configured then only object keys read from the queue will be downloaded.`,
		Description: `
If an SQS queue is not specified the entire list of objects found when this input starts will be consumed.

If your bucket is configured to send events directly to an SQS queue then you need to set the ` + "`sqs_body_path`" + ` field to a [dot path](/docs/configuration/field_paths) where the object key is found in the payload. However, it is also common practice to send bucket events to an SNS topic which sends enveloped events to SQS, in which case you must also set the ` + "`sqs_envelope_path`" + ` field to where the payload can be found.

When using SQS events it's also possible to extract target bucket names from the events by specifying a path in the field ` + "`sqs_bucket_path`" + `. For each SQS event, if that path exists and contains a string it will used as the bucket of the download instead of the ` + "`bucket`" + ` field.

Here is a guide for setting up an SQS queue that receives events for new S3 bucket objects:

https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html

WARNING: When using SQS please make sure you have sensible values for ` + "`sqs_max_messages`" + ` and also the visibility timeout of the queue itself.

When Benthos consumes an S3 item as a result of receiving an SQS message the message is not deleted until the S3 item has been sent onwards. This ensures at-least-once crash resiliency, but also means that if the S3 item takes longer to process than the visibility timeout of your queue then the same items might be processed multiple times.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/aws).

### Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- aws_s3_key
- aws_s3_bucket
- aws_s3_last_modified_unix
- aws_s3_last_modified (RFC3339)
- aws_s3_content_type
- aws_s3_content_encoding
- All user defined metadata
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: append(
			append(docs.FieldSpecs{
				docs.FieldCommon("bucket", "The bucket to consume from. If `sqs_bucket_path` is set this field is still required as a fallback."),
				docs.FieldCommon("prefix", "An optional path prefix, if set only objects with the prefix are consumed. This field is ignored when SQS is used."),
			}, sess.FieldSpecs()...),
			docs.FieldAdvanced("retries", "The maximum number of times to attempt an object download."),
			docs.FieldAdvanced("force_path_style_urls", "Forces the client API to use path style URLs, which helps when connecting to custom endpoints."),
			docs.FieldAdvanced("delete_objects", "Whether to delete downloaded objects from the bucket."),
			docs.FieldCommon("sqs", "Consume SQS messages in order to trigger key downloads.").WithChildren(
				docs.FieldCommon("url", "An optional SQS URL to connect to. When specified this queue will control which objects are downloaded from the target bucket."),
				docs.FieldAdvanced("endpoint", "A custom endpoint to use when connecting to SQS."),
				docs.FieldCommon("key_path", "A [dot path](/docs/configuration/field_paths) whereby object keys are found in SQS messages, this field is only required when an `sqs_url` is specified."),
				docs.FieldCommon("bucket_path", "An optional [dot path](/docs/configuration/field_paths) whereby the bucket of an object can be found in consumed SQS messages."),
				docs.FieldCommon("envelope_path", "An optional [dot path](/docs/configuration/field_paths) of enveloped payloads to extract from SQS messages. This is required when pushing events from S3 to SNS to SQS."),
				docs.FieldAdvanced("max_messages", "The maximum number of SQS messages to consume from each request."),
			),
		),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

// AWSS3SQSConfig contains configuration for hooking up the S3 input with an SQS queue.
type AWSS3SQSConfig struct {
	URL          string `json:"url" yaml:"url"`
	Endpoint     string `json:"endpoint" yaml:"endpoint"`
	EnvelopePath string `json:"envelope_path" yaml:"envelope_path"`
	KeyPath      string `json:"key_path" yaml:"key_path"`
	BucketPath   string `json:"bucket_path" yaml:"bucket_path"`
	MaxMessages  int64  `json:"max_messages" yaml:"max_messages"`
}

// NewAWSS3SQSConfig creates a new AWSS3SQSConfig with default values.
func NewAWSS3SQSConfig() AWSS3SQSConfig {
	return AWSS3SQSConfig{
		URL:          "",
		Endpoint:     "",
		EnvelopePath: "Message",
		KeyPath:      "Records.*.s3.object.key",
		BucketPath:   "Records.*.s3.bucket.name",
		MaxMessages:  10,
	}
}

// AWSS3Config contains configuration values for the aws_s3 input type.
type AWSS3Config struct {
	sess.Config        `json:",inline" yaml:",inline"`
	Bucket             string         `json:"bucket" yaml:"bucket"`
	Prefix             string         `json:"prefix" yaml:"prefix"`
	Retries            int            `json:"retries" yaml:"retries"`
	ForcePathStyleURLs bool           `json:"force_path_style_urls" yaml:"force_path_style_urls"`
	DeleteObjects      bool           `json:"delete_objects" yaml:"delete_objects"`
	SQS                AWSS3SQSConfig `json:"sqs" yaml:"sqs"`
}

// NewAWSS3Config creates a new AWSS3Config with default values.
func NewAWSS3Config() AWSS3Config {
	return AWSS3Config{
		Config:             sess.NewConfig(),
		Bucket:             "",
		Prefix:             "",
		Retries:            3,
		ForcePathStyleURLs: false,
		DeleteObjects:      false,
		SQS:                NewAWSS3SQSConfig(),
	}
}

//------------------------------------------------------------------------------

type objKey struct {
	s3Key     string
	s3Bucket  string
	attempts  int
	sqsHandle *sqs.DeleteMessageBatchRequestEntry
}

// AmazonS3 is a benthos reader.Type implementation that reads messages from an
// Amazon S3 bucket.
type amazonS3 struct {
	conf AWSS3Config

	sqsBodyPath   string
	sqsEnvPath    string
	sqsBucketPath string

	readKeys      []objKey
	targetKeys    []objKey
	targetKeysMut sync.Mutex

	session *session.Session
	s3      *s3.S3
	sqs     *sqs.SQS

	log   log.Modular
	stats metrics.Type
}

// NewAmazonS3 creates a new Amazon S3 bucket reader.Type.
func newAmazonS3(
	conf AWSS3Config,
	log log.Modular,
	stats metrics.Type,
) (*amazonS3, error) {
	if len(conf.SQS.URL) > 0 && conf.SQS.KeyPath == "Records.s3.object.key" {
		log.Warnf("It looks like a deprecated SQS Body path is configured: 'Records.s3.object.key', you might not receive S3 items unless you update to the new syntax 'Records.*.s3.object.key'")
	}

	if len(conf.Bucket) == 0 {
		return nil, errors.New("a bucket must be specified (even with an SQS bucket path configured)")
	}

	s := &amazonS3{
		conf:          conf,
		sqsBodyPath:   conf.SQS.KeyPath,
		sqsEnvPath:    conf.SQS.EnvelopePath,
		sqsBucketPath: conf.SQS.BucketPath,
		log:           log,
		stats:         stats,
	}
	return s, nil
}

// ConnectWithContext attempts to establish a connection to the target S3 bucket
// and any relevant queues used to traverse the objects (SQS, etc).
func (a *amazonS3) ConnectWithContext(ctx context.Context) error {
	a.targetKeysMut.Lock()
	defer a.targetKeysMut.Unlock()

	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession(func(c *aws.Config) {
		c.S3ForcePathStyle = aws.Bool(a.conf.ForcePathStyleURLs)
	})
	if err != nil {
		return err
	}

	sThree := s3.New(sess)

	if len(a.conf.SQS.URL) == 0 {
		listInput := &s3.ListObjectsInput{
			Bucket: aws.String(a.conf.Bucket),
		}
		if len(a.conf.Prefix) > 0 {
			listInput.Prefix = aws.String(a.conf.Prefix)
		}
		err := sThree.ListObjectsPagesWithContext(ctx, listInput,
			func(page *s3.ListObjectsOutput, isLastPage bool) bool {
				for _, obj := range page.Contents {
					a.targetKeys = append(a.targetKeys, objKey{
						s3Key:    *obj.Key,
						attempts: a.conf.Retries,
					})
				}
				return true
			},
		)
		if err != nil {
			return fmt.Errorf("failed to list objects: %v", err)
		}
	} else {
		sqsSess := sess.Copy()
		if len(a.conf.SQS.Endpoint) > 0 {
			sqsSess.Config.Endpoint = &a.conf.SQS.Endpoint
		}
		a.sqs = sqs.New(sqsSess)
	}

	a.log.Infof("Receiving Amazon S3 objects from bucket: %s\n", a.conf.Bucket)

	a.session = sess
	a.s3 = sThree
	return nil
}

func digStrsFromSlices(slice []interface{}) []string {
	var strs []string
	for _, v := range slice {
		switch t := v.(type) {
		case []interface{}:
			strs = append(strs, digStrsFromSlices(t)...)
		case string:
			strs = append(strs, t)
		}
	}
	return strs
}

type objTarget struct {
	key    string
	bucket string
}

func (a *amazonS3) parseItemPaths(sqsMsg *string) ([]objTarget, error) {
	gObj, err := gabs.ParseJSON([]byte(*sqsMsg))
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQS message: %v", err)
	}

	if len(a.sqsEnvPath) > 0 {
		switch t := gObj.Path(a.sqsEnvPath).Data().(type) {
		case string:
			if gObj, err = gabs.ParseJSON([]byte(t)); err != nil {
				return nil, fmt.Errorf("failed to parse SQS message envelope: %v", err)
			}
		case []interface{}:
			docs := []interface{}{}
			strs := digStrsFromSlices(t)
			for _, v := range strs {
				var gObj2 interface{}
				if err2 := json.Unmarshal([]byte(v), &gObj2); err2 == nil {
					docs = append(docs, gObj2)
				}
			}
			if len(docs) == 0 {
				return nil, errors.New("couldn't locate S3 items from SQS message")
			}
			gObj = gabs.Wrap(docs)
		default:
			return nil, fmt.Errorf("unexpected envelope value: %v", t)
		}
	}

	var buckets []string
	switch t := gObj.Path(a.sqsBucketPath).Data().(type) {
	case string:
		buckets = []string{t}
	case []interface{}:
		buckets = digStrsFromSlices(t)
	}

	items := []objTarget{}

	switch t := gObj.Path(a.sqsBodyPath).Data().(type) {
	case string:
		if strings.HasPrefix(t, a.conf.Prefix) {
			bucket := ""
			if len(buckets) > 0 {
				bucket = buckets[0]
			}
			items = append(items, objTarget{
				key:    t,
				bucket: bucket,
			})
		}
	case []interface{}:
		newTargets := []string{}
		strs := digStrsFromSlices(t)
		for _, p := range strs {
			if strings.HasPrefix(p, a.conf.Prefix) {
				newTargets = append(newTargets, p)
			}
		}
		if len(newTargets) > 0 {
			for i, target := range newTargets {
				bucket := ""
				if len(buckets) > i {
					bucket = buckets[i]
				}
				decodedTarget, err := url.QueryUnescape(target)
				if err != nil {
					return nil, fmt.Errorf("failed to decode S3 path: %v", err)
				}
				items = append(items, objTarget{
					key:    decodedTarget,
					bucket: bucket,
				})
			}
		} else {
			return nil, errors.New("no items found in SQS message at specified path")
		}
	default:
		return nil, errors.New("no items found in SQS message at specified path")
	}
	return items, nil
}

func (a *amazonS3) rejectObjects(ctx context.Context, keys []objKey) {
	var failedMessageHandles []*sqs.ChangeMessageVisibilityBatchRequestEntry
	for _, key := range keys {
		failedMessageHandles = append(failedMessageHandles, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                key.sqsHandle.Id,
			ReceiptHandle:     key.sqsHandle.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
	}
	for len(failedMessageHandles) > 0 {
		input := sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(a.conf.SQS.URL),
			Entries:  failedMessageHandles,
		}

		// trim input entries to max size
		if len(failedMessageHandles) > 10 {
			input.Entries, failedMessageHandles = failedMessageHandles[:10], failedMessageHandles[10:]
		} else {
			failedMessageHandles = nil
		}
		if _, err := a.sqs.ChangeMessageVisibilityBatchWithContext(ctx, &input); err != nil {
			a.log.Errorf("Failed to reject SQS message: %v\n", err)
		}
	}
}

func (a *amazonS3) deleteObjects(ctx context.Context, keys []objKey) {
	deleteHandles := []*sqs.DeleteMessageBatchRequestEntry{}
	for _, key := range keys {
		if a.conf.DeleteObjects {
			bucket := a.conf.Bucket
			if len(key.s3Bucket) > 0 {
				bucket = key.s3Bucket
			}
			if _, serr := a.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key.s3Key),
			}); serr != nil {
				a.log.Errorf("Failed to delete consumed object: %v\n", serr)
			}
		}
		if key.sqsHandle != nil {
			deleteHandles = append(deleteHandles, key.sqsHandle)
		}
	}
	for len(deleteHandles) > 0 {
		input := sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(a.conf.SQS.URL),
			Entries:  deleteHandles,
		}

		// trim input entries to max size
		if len(deleteHandles) > 10 {
			input.Entries, deleteHandles = deleteHandles[:10], deleteHandles[10:]
		} else {
			deleteHandles = nil
		}

		if res, serr := a.sqs.DeleteMessageBatchWithContext(ctx, &input); serr != nil {
			a.log.Errorf("Failed to delete consumed SQS messages: %v\n", serr)
		} else {
			for _, fail := range res.Failed {
				a.log.Errorf("Failed to delete consumed SQS message '%v', response code: %v\n", *fail.Id, *fail.Code)
			}
		}
	}
}

func (a *amazonS3) readSQSEvents(ctx context.Context) error {
	var dudMessageHandles []*sqs.ChangeMessageVisibilityBatchRequestEntry
	addDudFn := func(m *sqs.Message) {
		dudMessageHandles = append(dudMessageHandles, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                m.MessageId,
			ReceiptHandle:     m.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
	}

	output, err := a.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(a.conf.SQS.URL),
		MaxNumberOfMessages: aws.Int64(a.conf.SQS.MaxMessages),
	})
	if err != nil {
		return err
	}

messageLoop:
	for _, sqsMsg := range output.Messages {
		msgHandle := &sqs.DeleteMessageBatchRequestEntry{
			Id:            sqsMsg.MessageId,
			ReceiptHandle: sqsMsg.ReceiptHandle,
		}

		if sqsMsg.Body == nil {
			addDudFn(sqsMsg)
			a.log.Errorln("Received empty SQS message")
			continue messageLoop
		}

		items, err := a.parseItemPaths(sqsMsg.Body)
		if err != nil {
			addDudFn(sqsMsg)
			a.log.Errorf("SQS error: %v\n", err)
			continue messageLoop
		}

		for _, item := range items {
			a.targetKeys = append(a.targetKeys, objKey{
				s3Key:    item.key,
				s3Bucket: item.bucket,
				attempts: a.conf.Retries,
			})
		}
		a.targetKeys[len(a.targetKeys)-1].sqsHandle = msgHandle
	}

	// Discard any SQS messages not associated with a target file.
	for len(dudMessageHandles) > 0 {
		input := sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(a.conf.SQS.URL),
			Entries:  dudMessageHandles,
		}

		// trim input entries to max size
		if len(dudMessageHandles) > 10 {
			input.Entries, dudMessageHandles = dudMessageHandles[:10], dudMessageHandles[10:]
		} else {
			dudMessageHandles = nil
		}
		a.sqs.ChangeMessageVisibilityBatch(&input)
	}

	if len(a.targetKeys) == 0 {
		return types.ErrTimeout
	}
	return nil
}

func (a *amazonS3) pushReadKey(key objKey) {
	a.readKeys = append(a.readKeys, key)
}

func (a *amazonS3) popTargetKey() {
	if len(a.targetKeys) == 0 {
		return
	}
	if len(a.targetKeys) > 1 {
		a.targetKeys = a.targetKeys[1:]
	} else {
		a.targetKeys = nil
	}
}

// ReadWithContext attempts to read a new message from the target S3 bucket.
func (a *amazonS3) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	a.targetKeysMut.Lock()
	defer a.targetKeysMut.Unlock()

	if a.session == nil {
		return nil, nil, types.ErrNotConnected
	}

	if len(a.targetKeys) == 0 {
		if a.sqs != nil {
			if err := a.readSQSEvents(ctx); err != nil {
				return nil, nil, err
			}
		} else {
			// If we aren't using SQS but exhausted our targets we are done.
			return nil, nil, types.ErrTypeClosed
		}
	}
	if len(a.targetKeys) == 0 {
		return nil, nil, types.ErrTimeout
	}

	msg := message.New(nil)

	part, obj, err := a.read(ctx)
	if err != nil {
		return nil, nil, err
	}

	msg.Append(part)
	return msg, func(rctx context.Context, res types.Response) error {
		if res.Error() == nil {
			a.deleteObjects(ctx, []objKey{obj})
		} else {
			if len(a.conf.SQS.URL) == 0 {
				a.targetKeysMut.Lock()
				a.targetKeys = append(a.readKeys, obj)
				a.targetKeysMut.Unlock()
			} else {
				a.rejectObjects(ctx, []objKey{obj})
			}
		}
		return nil
	}, nil
}

func addS3Metadata(p types.Part, obj *s3.GetObjectOutput) {
	meta := p.Metadata()
	if obj.LastModified != nil {
		meta.Set("aws_s3_last_modified", obj.LastModified.Format(time.RFC3339))
		meta.Set("aws_s3_last_modified_unix", strconv.FormatInt(obj.LastModified.Unix(), 10))
	}
	if obj.ContentType != nil {
		meta.Set("aws_s3_content_type", *obj.ContentType)
	}
	if obj.ContentEncoding != nil {
		meta.Set("aws_s3_content_encoding", *obj.ContentEncoding)
	}
}

// read attempts to read a new message from the target S3 bucket.
func (a *amazonS3) read(ctx context.Context) (types.Part, objKey, error) {
	target := a.targetKeys[0]

	bucket := a.conf.Bucket
	if len(target.s3Bucket) > 0 {
		bucket = target.s3Bucket
	}
	obj, err := a.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(target.s3Key),
	})
	if err != nil {
		target.attempts--
		if target.attempts == 0 {
			// Remove the target file from our list.
			a.popTargetKey()
			a.log.Errorf("Failed to download file '%s' from bucket '%s' after '%v' attempts: %v\n", target.s3Key, bucket, a.conf.Retries, err)
		} else {
			a.targetKeys[0] = target
			return nil, objKey{}, fmt.Errorf("failed to download file '%s' from bucket '%s': %v", target.s3Key, bucket, err)
		}
		return nil, objKey{}, types.ErrTimeout
	}

	bytes, err := ioutil.ReadAll(obj.Body)
	obj.Body.Close()
	if err != nil {
		a.popTargetKey()
		return nil, objKey{}, fmt.Errorf("failed to download file '%s' from bucket '%s': %v", target.s3Key, bucket, err)
	}

	part := message.NewPart(bytes)
	meta := part.Metadata()
	for k, v := range obj.Metadata {
		meta.Set(k, *v)
	}
	meta.Set("aws_s3_key", target.s3Key)
	meta.Set("aws_s3_bucket", bucket)
	addS3Metadata(part, obj)

	a.popTargetKey()
	return part, target, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *amazonS3) CloseAsync() {
	go func() {
		a.targetKeysMut.Lock()
		a.rejectObjects(context.Background(), a.targetKeys)
		a.targetKeys = nil
		a.targetKeysMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *amazonS3) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
