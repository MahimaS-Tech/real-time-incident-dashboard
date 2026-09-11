//go:build production

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"incident-dashboard/pkg/model"

	"github.com/redis/go-redis/v9"
)

const liveChannel = "incidents.live"

type RedisStore struct {
	client    *redis.Client
	dedupeTTL time.Duration
}

func NewRedisStore(addr, password string, db int, dedupeTTL time.Duration) *RedisStore {
	return &RedisStore{
		client:    redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db}),
		dedupeTTL: dedupeTTL,
	}
}

func (s *RedisStore) Close() error { return s.client.Close() }

func (s *RedisStore) Ping(ctx context.Context) error { return s.client.Ping(ctx).Err() }

var upsertScript = redis.NewScript(`
local event_ttl = tonumber(ARGV[1])
local new_id = ARGV[2]
local dedupe_key = ARGV[3]
local source = ARGV[4]
local external_id = ARGV[5]
local service = ARGV[6]
local region = ARGV[7]
local signal = ARGV[8]
local message = ARGV[9]
local severity = ARGV[10]
local status = ARGV[11]
local ts = ARGV[12]
local score = ARGV[13]
local status_code = ARGV[14]
local latency_ms = ARGV[15]
local error_rate = ARGV[16]
local attributes = ARGV[17]
local severity_rank = tonumber(ARGV[18])

local processed = redis.call('SET', KEYS[1], '1', 'NX', 'EX', event_ttl)
if not processed then
  local duplicate_id = redis.call('GET', KEYS[2])
  if duplicate_id then
    return {0, duplicate_id}
  end
  return {0, ''}
end

local id = redis.call('GET', KEYS[2])
local state = 2
if not id then
  id = new_id
  state = 1
  redis.call('SET', KEYS[2], id, 'EX', event_ttl)
  redis.call('HSET', 'incident:' .. id,
    'id', id,
    'dedupe_key', dedupe_key,
    'source', source,
    'external_id', external_id,
    'service', service,
    'region', region,
    'signal', signal,
    'message', message,
    'severity', severity,
    'severity_rank', severity_rank,
    'status', status,
    'status_code', status_code,
    'latency_ms', latency_ms,
    'error_rate', error_rate,
    'count', 1,
    'first_seen', ts,
    'last_seen', ts,
    'attributes', attributes)
  if status == 'open' then
    redis.call('SADD', 'incidents:open', id)
  end
else
  redis.call('HINCRBY', 'incident:' .. id, 'count', 1)
  local existing_rank = tonumber(redis.call('HGET', 'incident:' .. id, 'severity_rank') or '0')
  if severity_rank >= existing_rank then
    redis.call('HSET', 'incident:' .. id, 'severity', severity, 'severity_rank', severity_rank)
  end
  redis.call('HSET', 'incident:' .. id,
    'message', message,
    'status', status,
    'status_code', status_code,
    'latency_ms', latency_ms,
    'error_rate', error_rate,
    'last_seen', ts,
    'attributes', attributes)
  if status == 'resolved' then
    redis.call('SREM', 'incidents:open', id)
  else
    redis.call('SADD', 'incidents:open', id)
  end
end

redis.call('ZADD', 'incidents:last_seen', score, id)
redis.call('INCR', 'metrics:events_total')
redis.call('INCR', 'metrics:severity:' .. severity)
return {state, id}
`)

func (s *RedisStore) UpsertIncident(ctx context.Context, event model.IncidentEvent) (model.Incident, bool, error) {
	attrs, _ := json.Marshal(event.Attributes)
	status := model.StatusOpen
	if event.Severity == model.SeverityResolved {
		status = model.StatusResolved
	}
	dedupe := event.DedupeKey()
	ttlSeconds := int64(s.dedupeTTL.Seconds())
	if ttlSeconds <= 0 {
		ttlSeconds = int64((7 * 24 * time.Hour).Seconds())
	}
	keys := []string{"event:processed:" + event.EventID, "incident:dedupe:" + dedupe}
	args := []any{
		ttlSeconds,
		model.NewID(),
		dedupe,
		event.Source,
		event.ExternalID,
		event.Service,
		event.Region,
		event.Signal,
		event.Message,
		event.Severity,
		status,
		event.Timestamp.UTC().Format(time.RFC3339Nano),
		event.Timestamp.UTC().UnixNano(),
		event.StatusCode,
		fmt.Sprintf("%.3f", event.LatencyMS),
		fmt.Sprintf("%.6f", event.ErrorRate),
		string(attrs),
		model.SeverityRank(event.Severity),
	}
	result, err := upsertScript.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return model.Incident{}, false, err
	}
	items, ok := result.([]any)
	if !ok || len(items) < 2 {
		return model.Incident{}, false, fmt.Errorf("unexpected redis script result: %#v", result)
	}
	state, _ := strconv.ParseInt(fmt.Sprint(items[0]), 10, 64)
	id := fmt.Sprint(items[1])
	if id == "" {
		return model.Incident{}, false, nil
	}
	incident, err := s.getIncident(ctx, id)
	if err != nil {
		return model.Incident{}, false, err
	}
	return incident, state != 0, nil
}

func (s *RedisStore) ListOpenIncidents(ctx context.Context, limit int) ([]model.Incident, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	ids, err := s.client.ZRevRange(ctx, "incidents:last_seen", 0, int64(limit*5)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]model.Incident, 0, limit)
	for _, id := range ids {
		incident, err := s.getIncident(ctx, id)
		if err != nil {
			continue
		}
		if incident.Status == model.StatusOpen {
			out = append(out, incident)
		}
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *RedisStore) Metrics(ctx context.Context) (Metrics, error) {
	pipe := s.client.Pipeline()
	total := pipe.Get(ctx, "metrics:events_total")
	open := pipe.SCard(ctx, "incidents:open")
	critical := pipe.Get(ctx, "metrics:severity:"+model.SeverityCritical)
	warning := pipe.Get(ctx, "metrics:severity:"+model.SeverityWarning)
	info := pipe.Get(ctx, "metrics:severity:"+model.SeverityInfo)
	resolved := pipe.Get(ctx, "metrics:severity:"+model.SeverityResolved)
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return Metrics{}, err
	}
	return Metrics{
		EventsTotal:   stringIntValue(total),
		OpenIncidents: redisIntValue(open),
		BySeverity: map[string]int64{
			model.SeverityCritical: stringIntValue(critical),
			model.SeverityWarning:  stringIntValue(warning),
			model.SeverityInfo:     stringIntValue(info),
			model.SeverityResolved: stringIntValue(resolved),
		},
	}, nil
}

func (s *RedisStore) PublishIncident(ctx context.Context, incident model.Incident) error {
	payload, err := json.Marshal(incident)
	if err != nil {
		return err
	}
	return s.client.Publish(ctx, liveChannel, payload).Err()
}

func (s *RedisStore) SubscribeIncidents(ctx context.Context) (<-chan model.Incident, func() error, error) {
	pubsub := s.client.Subscribe(ctx, liveChannel)
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, nil, err
	}
	out := make(chan model.Incident, 1024)
	go func() {
		defer close(out)
		ch := pubsub.Channel(redis.WithChannelSize(1024))
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var incident model.Incident
				if err := json.Unmarshal([]byte(msg.Payload), &incident); err == nil {
					select {
					case out <- incident:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, pubsub.Close, nil
}

func (s *RedisStore) getIncident(ctx context.Context, id string) (model.Incident, error) {
	values, err := s.client.HGetAll(ctx, "incident:"+id).Result()
	if err != nil {
		return model.Incident{}, err
	}
	if len(values) == 0 {
		return model.Incident{}, fmt.Errorf("incident %s not found", id)
	}
	return incidentFromHash(values)
}

func incidentFromHash(values map[string]string) (model.Incident, error) {
	count, _ := strconv.ParseInt(values["count"], 10, 64)
	statusCode, _ := strconv.Atoi(values["status_code"])
	latency, _ := strconv.ParseFloat(values["latency_ms"], 64)
	errorRate, _ := strconv.ParseFloat(values["error_rate"], 64)
	firstSeen, _ := time.Parse(time.RFC3339Nano, values["first_seen"])
	lastSeen, _ := time.Parse(time.RFC3339Nano, values["last_seen"])
	attrs := map[string]string{}
	_ = json.Unmarshal([]byte(values["attributes"]), &attrs)
	if len(attrs) == 0 {
		attrs = nil
	}
	return model.Incident{
		ID:         values["id"],
		DedupeKey:  values["dedupe_key"],
		Source:     values["source"],
		ExternalID: values["external_id"],
		Service:    values["service"],
		Region:     values["region"],
		Signal:     values["signal"],
		Message:    values["message"],
		Severity:   values["severity"],
		Status:     values["status"],
		StatusCode: statusCode,
		LatencyMS:  latency,
		ErrorRate:  errorRate,
		Count:      count,
		FirstSeen:  firstSeen,
		LastSeen:   lastSeen,
		Attributes: attrs,
	}, nil
}

func stringIntValue(cmd *redis.StringCmd) int64 {
	v, err := cmd.Int64()
	if err != nil {
		return 0
	}
	return v
}

func redisIntValue(cmd *redis.IntCmd) int64 {
	v, err := cmd.Result()
	if err != nil {
		return 0
	}
	return v
}
