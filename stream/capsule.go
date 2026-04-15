package stream

import (
	"fmt"
	"reflect"

	"github.com/zclconf/go-cty/cty"
)

// ConsumerCapsuleType wraps a *RedisStreamConsumer for HCL. It lets the
// redis_ack() function extract the consumer from an address like
// `client.rs.consumer.in` without creating a dependency from vinculum/config
// onto the stream runtime.
var ConsumerCapsuleType = cty.CapsuleWithOps("redis_stream_consumer",
	reflect.TypeOf(RedisStreamConsumer{}),
	&cty.CapsuleOps{
		GoString: func(val interface{}) string {
			return fmt.Sprintf("redis_stream_consumer(%p)", val)
		},
		TypeGoString: func(_ reflect.Type) string {
			return "RedisStreamConsumer"
		},
	})

// NewConsumerCapsule wraps a consumer for use in the HCL eval context.
func NewConsumerCapsule(c *RedisStreamConsumer) cty.Value {
	return cty.CapsuleVal(ConsumerCapsuleType, c)
}

// GetConsumerFromCapsule extracts a *RedisStreamConsumer from a capsule.
func GetConsumerFromCapsule(v cty.Value) (*RedisStreamConsumer, error) {
	if v.Type() != ConsumerCapsuleType {
		return nil, fmt.Errorf("expected redis_stream_consumer capsule, got %s", v.Type().FriendlyName())
	}
	c, ok := v.EncapsulatedValue().(*RedisStreamConsumer)
	if !ok {
		return nil, fmt.Errorf("encapsulated value is not a RedisStreamConsumer, got %T", v.EncapsulatedValue())
	}
	return c, nil
}
