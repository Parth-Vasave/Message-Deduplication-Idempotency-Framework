# Message Deduplication & Idempotency Framework
## Claude Development Roadmap ⭐⭐⭐

---

## 📋 Project Overview

**Problem Statement:**  
Kafka consumers can receive duplicate messages due to network hiccups, broker failures, or consumer crashes. Without deduplication, this results in duplicate processing—orders created twice, payments charged twice, inventory decremented twice—causing revenue loss and data corruption.

**Solution:**  
A robust deduplication and idempotency framework that:
- Tracks processed message IDs to prevent duplicate work
- Ensures operations are idempotent (safe to retry)
- Cleans up old dedup records with TTL
- Monitors and alerts on duplicate activity
- Provides visibility into dedup performance

**Real Value:**
- ✅ Essential for any async/event-driven system
- ✅ Demonstrates understanding of distributed systems
- ✅ Prevents financial loss and data inconsistency
- ✅ Production-grade reliability pattern

---

## 🎯 Phase 1: Foundation & Core Architecture (Weeks 1-2)

### 1.1 Project Setup & Tech Stack Selection

**Objectives:**
- [ ] Define technology stack
- [ ] Set up local development environment
- [ ] Create project structure and boilerplate

**Tasks:**
```
Tech Stack Decision:
├── Runtime: Node.js / Python / Go
├── Message Queue: Kafka / RabbitMQ
├── Cache Layer: Redis (dedup storage)
├── Database: PostgreSQL (audit logs, metrics)
├── Framework: Express/FastAPI/Gin
└── Container: Docker + Docker Compose

Boilerplate:
├── Project skeleton (src/, tests/, docs/)
├── .gitignore, README.md, package.json
├── Docker Compose (Kafka + Redis + PostgreSQL)
└── Sample consumer skeleton
```

**Deliverables:**
- [ ] `docker-compose.yml` with all services
- [ ] Basic project structure
- [ ] Development setup documentation
- [ ] Local run guide

**Claude's Role:**
- Help define trade-offs between tech choices
- Generate boilerplate code and configs
- Create Docker setup scripts
- Document setup process

---

### 1.2 Core Deduplication Service Design

**Objectives:**
- [ ] Design dedup storage strategy
- [ ] Define message ID structure
- [ ] Plan Redis key schema
- [ ] Create dedup API interface

**Design Decisions:**

```
Message ID Strategy:
├── Format: {source}-{timestamp}-{sequenceId}
├── Uniqueness: Guaranteed by source + timestamp + seq
├── Length: <256 bytes (fits Redis key)
└── Examples:
    ├── order-1712000000000-001
    ├── payment-1712000001000-002
    └── shipment-1712000002000-003

Redis Key Schema:
├── dedup:{message_id} → {status, timestamp, result}
├── dedup:batch:{batch_id} → {count, processed_at}
├── dedup:metrics:{date} → {count, dedups_found}
└── TTL: 24-48 hours (configurable)

Dedup Status Values:
├── PROCESSING
├── COMPLETED
├── FAILED
└── ROLLBACK
```

**API Interface Design:**
```typescript
interface DeduplicationService {
  // Check if message already processed
  isDuplicate(messageId: string): Promise<boolean>
  
  // Mark message as being processed
  markProcessing(messageId: string): Promise<void>
  
  // Mark message as completed
  markCompleted(messageId: string, result: any): Promise<void>
  
  // Mark as failed
  markFailed(messageId: string, error: string): Promise<void>
  
  // Get processing status
  getStatus(messageId: string): Promise<ProcessingStatus | null>
  
  // Cleanup old records
  cleanupExpired(ttlSeconds: number): Promise<number>
}

interface IdempotencyService {
  // Execute function with idempotency
  executeIdempotent<T>(
    messageId: string,
    operation: () => Promise<T>
  ): Promise<T>
  
  // Rollback operation if needed
  rollback(messageId: string): Promise<void>
}
```

**Deliverables:**
- [ ] Design document (ASCII diagrams welcome)
- [ ] Key schema specification
- [ ] API interface definitions
- [ ] Status state machine diagram

**Claude's Role:**
- Help design Redis key schema for scalability
- Create state machine diagrams
- Define API contracts
- Suggest failure scenarios to handle

---

### 1.3 Implementation: Deduplication Storage Layer

**Objectives:**
- [ ] Implement Redis-based dedup storage
- [ ] Add local fallback for testing
- [ ] Create storage abstraction layer

**Code Structure:**
```
src/
├── storage/
│   ├── DeduplicationStore.ts (interface)
│   ├── RedisDeduplicationStore.ts
│   ├── InMemoryDeduplicationStore.ts (dev/test)
│   └── __tests__/
│       └── dedup.test.ts
├── types/
│   ├── Message.ts
│   ├── ProcessingStatus.ts
│   └── DeduplicationConfig.ts
└── config/
    └── redis.config.ts
```

**Implementation Checklist:**
- [ ] Redis connection pooling
- [ ] Atomic operations (Lua scripts for race conditions)
- [ ] TTL expiration setup
- [ ] Error handling and retries
- [ ] In-memory store for dev/testing
- [ ] Unit tests (90%+ coverage)

**Critical: Atomic Operations**
```lua
-- Example: Atomic mark-processing
-- Prevents race condition where two processes
-- both check && mark simultaneously
SCRIPT LOAD "
  local key = KEYS[1]
  local ttl = ARGV[1]
  
  if redis.call('EXISTS', key) == 1 then
    return 0  -- Already exists
  end
  
  redis.call('SET', key, 'PROCESSING', 'EX', ttl)
  return 1  -- Successfully marked
"
```

**Deliverables:**
- [ ] Deduplication storage implementation
- [ ] Test suite with mocks
- [ ] Redis Lua script utilities
- [ ] Configuration documentation

**Claude's Role:**
- Generate storage implementation code
- Help write Lua scripts for atomic operations
- Create comprehensive test suite
- Debug race conditions

---

## 🔄 Phase 2: Consumer Integration & Idempotency (Weeks 3-4)

### 2.1 Kafka Consumer Wrapper

**Objectives:**
- [ ] Create Kafka consumer with built-in dedup
- [ ] Implement graceful error handling
- [ ] Add circuit breaker pattern

**Implementation:**
```
src/consumers/
├── BaseConsumer.ts (abstract)
├── DedupConsumer.ts (with deduplication)
├── ConsumerConfig.ts
└── handlers/
    ├── OrderHandler.ts
    ├── PaymentHandler.ts
    └── ShipmentHandler.ts
```

**Consumer Lifecycle:**
```
1. Receive message from Kafka
   ↓
2. Extract & validate message ID
   ↓
3. Check dedup store (isDuplicate?)
   ├─ YES → Skip processing, commit offset
   └─ NO → Continue
   ↓
4. Mark as PROCESSING in dedup store
   ↓
5. Execute business logic (with idempotency)
   ↓
6. Mark as COMPLETED with result
   ↓
7. Commit Kafka offset
   ↓
8. On error → Mark FAILED & handle retry
```

**Code Example:**
```typescript
class DedupConsumer<T> {
  async processMessage(
    message: KafkaMessage,
    handler: MessageHandler<T>
  ): Promise<void> {
    const messageId = this.extractMessageId(message)
    
    // 1. Check if already processed
    const isDup = await this.dedup.isDuplicate(messageId)
    if (isDup) {
      log.info(`Duplicate detected: ${messageId}`)
      metrics.recordDuplicate()
      return
    }
    
    try {
      // 2. Mark as processing
      await this.dedup.markProcessing(messageId)
      
      // 3. Execute with idempotency guard
      const result = await this.idempotency.executeIdempotent(
        messageId,
        () => handler(message)
      )
      
      // 4. Mark completed
      await this.dedup.markCompleted(messageId, result)
      metrics.recordSuccess()
      
    } catch (error) {
      await this.dedup.markFailed(messageId, error.message)
      metrics.recordFailure()
      throw error // Will retry via Kafka rebalance
    }
  }
}
```

**Error Handling Strategy:**
```
Processing Error:
├── Transient (network timeout, temporary DB issue)
│   └── Retry: 3x with exponential backoff
├── Idempotent Safe (duplicate order ID, already processed)
│   └── Skip: Mark as completed, move on
├── Non-Idempotent (insufficient funds, stock error)
│   └── DLQ: Send to Dead Letter Queue for manual review
└── Critical (poison message, JSON invalid)
    └── Log + Skip to prevent blocking consumer
```

**Deliverables:**
- [ ] DedupConsumer implementation
- [ ] Error handling framework
- [ ] Unit tests for consumer logic
- [ ] Integration tests with Kafka test containers

**Claude's Role:**
- Generate consumer base class
- Create error handling patterns
- Write integration test suite
- Build message handler examples

---

### 2.2 Idempotency Guards

**Objectives:**
- [ ] Implement idempotency at operation level
- [ ] Create database-level idempotency keys
- [ ] Design rollback mechanism

**Idempotent Operation Patterns:**

```typescript
// Pattern 1: Database-level idempotency
async function createOrderIdempotent(
  messageId: string,
  orderData: OrderData
): Promise<Order> {
  return db.transaction(async (trx) => {
    // Attempt to insert with unique constraint on messageId
    const existing = await trx('orders')
      .where('dedup_message_id', messageId)
      .first()
    
    if (existing) {
      return existing  // Already created
    }
    
    const order = await trx('orders').insert({
      ...orderData,
      dedup_message_id: messageId,  // Idempotency key
      created_at: new Date()
    })
    
    return order
  })
}

// Pattern 2: Outbox pattern (for distributed txns)
async function createOrderWithOutbox(
  messageId: string,
  orderData: OrderData
) {
  return db.transaction(async (trx) => {
    const order = await trx('orders').insert({
      ...orderData,
      dedup_message_id: messageId
    })
    
    // Store event to publish later
    await trx('outbox').insert({
      aggregate_id: order.id,
      event_type: 'ORDER_CREATED',
      payload: JSON.stringify(order),
      dedup_message_id: messageId,
      published: false
    })
    
    return order
  })
}

// Pattern 3: Retrieve previous result on retry
async function processPaymentIdempotent(
  messageId: string,
  paymentData: PaymentData
): Promise<PaymentResult> {
  // Check if we have cached result from previous attempt
  const cachedResult = await cache.get(`payment:${messageId}`)
  if (cachedResult) {
    return JSON.parse(cachedResult)
  }
  
  const result = await paymentService.processPayment(paymentData)
  
  // Cache for idempotency (TTL: 48 hours)
  await cache.set(`payment:${messageId}`, JSON.stringify(result), 48 * 3600)
  
  return result
}
```

**Rollback Mechanism:**
```typescript
interface IdempotentOperation {
  messageId: string
  operation: () => Promise<any>
  rollback?: () => Promise<void>
  timeout: number
}

async function executeWithRollback(
  op: IdempotentOperation
): Promise<any> {
  try {
    const result = await Promise.race([
      op.operation(),
      sleep(op.timeout)
    ])
    return result
  } catch (error) {
    if (op.rollback) {
      log.warn(`Rolling back operation ${op.messageId}`)
      await op.rollback()
    }
    throw error
  }
}
```

**Deliverables:**
- [ ] Idempotency guard decorators
- [ ] Database schema with dedup_message_id columns
- [ ] Outbox pattern implementation
- [ ] Rollback utilities
- [ ] Migration scripts

**Claude's Role:**
- Generate idempotent operation patterns
- Create decorator helpers
- Write database migrations
- Suggest rollback strategies

---

## 📊 Phase 3: Monitoring, Metrics & Observability (Weeks 5-6)

### 3.1 Metrics Collection

**Objectives:**
- [ ] Track dedup metrics
- [ ] Monitor consumer health
- [ ] Build Prometheus metrics

**Key Metrics:**
```
Deduplication Metrics:
├── dedup_duplicates_detected_total
│   └── Labels: [topic, handler, status]
├── dedup_processing_time_seconds
│   └── Histogram: [1ms, 10ms, 100ms, 1s, 10s]
├── dedup_cache_hit_ratio
│   └── Gauge: [0-100%]
├── dedup_storage_items_total
│   └── Gauge: current size of dedup store
└── dedup_ttl_cleanup_records_removed
    └── Counter: cleanup runs

Consumer Health Metrics:
├── consumer_messages_processed_total
├── consumer_messages_failed_total
├── consumer_lag_seconds
├── consumer_processing_time_seconds
└── consumer_errors_by_type

Business Metrics:
├── orders_created_duplicate_attempts
├── payments_retried_total
├── revenue_saved_by_dedup (estimate)
└── dlq_messages_total
```

**Implementation:**
```typescript
import { Registry, Counter, Gauge, Histogram } from 'prom-client'

class DeduplicationMetrics {
  private registry = new Registry()
  
  duplicatesDetected = new Counter({
    name: 'dedup_duplicates_detected_total',
    help: 'Total duplicates detected',
    labelNames: ['topic', 'handler', 'status'],
    registers: [this.registry]
  })
  
  processingTime = new Histogram({
    name: 'dedup_processing_time_seconds',
    help: 'Processing time for operations',
    buckets: [0.001, 0.01, 0.1, 1, 10],
    registers: [this.registry]
  })
  
  storageItems = new Gauge({
    name: 'dedup_storage_items_total',
    help: 'Current items in dedup store',
    registers: [this.registry]
  })
  
  recordDuplicate(topic: string, handler: string): void {
    this.duplicatesDetected.inc({
      topic,
      handler,
      status: 'skipped'
    })
  }
  
  recordProcessing(duration: number): void {
    this.processingTime.observe(duration / 1000)
  }
  
  getMetrics(): string {
    return this.registry.metrics()
  }
}
```

**Deliverables:**
- [ ] Prometheus metrics implementation
- [ ] Custom metrics helper library
- [ ] Metrics documentation

**Claude's Role:**
- Generate metrics collection code
- Define metric thresholds
- Create Prometheus queries

---

### 3.2 Dashboard & Visualization

**Objectives:**
- [ ] Create Grafana dashboard
- [ ] Build real-time monitoring UI
- [ ] Set up alerting rules

**Dashboard Panels:**

```
Dashboard: "Message Deduplication & Idempotency"

Row 1: Overview
├── Panel: Dedup Hit Rate (%)
│   └── Query: dedup_duplicates_detected_total / total_messages
├── Panel: Messages Processed (24h)
│   └── Query: rate(consumer_messages_processed_total[24h])
├── Panel: Duplicate Detections (24h)
│   └── Query: increase(dedup_duplicates_detected_total[24h])
└── Panel: Consumer Lag (seconds)
    └── Query: consumer_lag_seconds

Row 2: Deduplication Details
├── Panel: Duplicate Detection by Topic
│   └── Type: Bar chart
│   └── Query: sum by (topic) (dedup_duplicates_detected_total)
├── Panel: Processing Time Distribution
│   └── Type: Histogram
│   └── Query: dedup_processing_time_seconds
├── Panel: Storage Items Trend
│   └── Type: Time series
│   └── Query: dedup_storage_items_total
└── Panel: TTL Cleanup Activity
    └── Type: Time series
    └── Query: rate(dedup_ttl_cleanup_records_removed[5m])

Row 3: Errors & Health
├── Panel: Failed Processings (24h)
│   └── Query: increase(consumer_messages_failed_total[24h])
├── Panel: DLQ Messages
│   └── Query: dlq_messages_total
├── Panel: Processing Time P99
│   └── Query: histogram_quantile(0.99, dedup_processing_time_seconds)
└── Panel: Cache Hit Ratio
    └── Query: dedup_cache_hit_ratio

Alerts:
├── HighDuplicateRate: dedup_duplicates_detected_total > 5% of total
├── ConsumerLag: consumer_lag_seconds > 300
├── DeduplicationFailure: consumer_messages_failed_total increases
├── StorageFull: dedup_storage_items_total > 1M items
└── CleanupFailure: increase(dedup_ttl_cleanup_records_removed[1h]) == 0
```

**Grafana JSON (snippet):**
```json
{
  "dashboard": {
    "title": "Message Deduplication & Idempotency",
    "tags": ["kafka", "dedup", "idempotency"],
    "panels": [
      {
        "title": "Dedup Hit Rate",
        "targets": [
          {
            "expr": "dedup_duplicates_detected_total / on() total_messages_processed"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "percentunit",
            "max": 1,
            "min": 0
          }
        }
      }
    ]
  }
}
```

**Deliverables:**
- [ ] Grafana dashboard JSON
- [ ] Alert rule definitions (Alertmanager)
- [ ] Dashboard documentation
- [ ] Runbook for alerts

**Claude's Role:**
- Generate Grafana dashboard JSON
- Create alert rules
- Write runbooks for on-call

---

### 3.3 Logging & Tracing

**Objectives:**
- [ ] Implement structured logging
- [ ] Add distributed tracing
- [ ] Create log aggregation pipeline

**Structured Logging:**
```typescript
logger.info('Dedup check', {
  messageId: '123',
  isDuplicate: true,
  status: 'SKIPPED',
  duration_ms: 5,
  topic: 'orders',
  timestamp: new Date().toISOString()
})

logger.error('Dedup processing failed', {
  messageId: '123',
  error: 'Database connection timeout',
  attempts: 3,
  stack_trace: '...',
  severity: 'HIGH'
})
```

**Distributed Tracing (OpenTelemetry):**
```typescript
import { trace } from '@opentelemetry/api'

const tracer = trace.getTracer('dedup-consumer')

async function processMessage(message) {
  const span = tracer.startSpan('processMessage', {
    attributes: {
      'messaging.message_id': messageId,
      'messaging.destination': topic,
      'messaging.operation': 'process'
    }
  })
  
  try {
    // ... processing
  } finally {
    span.end()
  }
}
```

**Deliverables:**
- [ ] Structured logging setup (Winston/Pino)
- [ ] OpenTelemetry instrumentation
- [ ] Log aggregation (ELK Stack / Loki)
- [ ] Trace exporters

**Claude's Role:**
- Generate logging middleware
- Create OpenTelemetry setup
- Write log queries

---

## 🧪 Phase 4: Testing & Resilience (Weeks 7-8)

### 4.1 Comprehensive Test Suite

**Objectives:**
- [ ] Unit tests (90%+ coverage)
- [ ] Integration tests
- [ ] Chaos/failure injection tests

**Test Categories:**

```
tests/
├── unit/
│   ├── dedup.store.test.ts (100+ tests)
│   ├── dedup.consumer.test.ts
│   ├── idempotency.test.ts
│   └── metrics.test.ts
├── integration/
│   ├── kafka.integration.test.ts
│   │   └── Test: duplicate message → single processing
│   ├── redis.integration.test.ts
│   │   └── Test: TTL cleanup, race conditions
│   └── database.integration.test.ts
│       └── Test: idempotent writes, constraints
├── chaos/
│   ├── network.failure.test.ts
│   ├── redis.failure.test.ts
│   ├── kafka.timeout.test.ts
│   └── partial.failure.test.ts
└── e2e/
    ├── order.workflow.test.ts
    │   └── End-to-end: Create order → Deduplicate → Success
    ├── payment.retry.test.ts
    │   └── End-to-end: Payment retry scenarios
    └── dlq.handling.test.ts
        └── End-to-end: Poison messages → DLQ
```

**Sample Tests:**

```typescript
// Unit: Dedup store detects duplicates
describe('DeduplicationStore', () => {
  it('should detect duplicate message ID', async () => {
    const messageId = 'order-1'
    await store.markProcessing(messageId)
    
    const isDup = await store.isDuplicate(messageId)
    expect(isDup).toBe(true)
  })
  
  it('should handle concurrent processing safely', async () => {
    const messageId = 'order-2'
    
    // Simulate 10 concurrent processes checking simultaneously
    const promises = Array(10).fill(null).map(() =>
      store.markProcessing(messageId)
    )
    
    // Only one should succeed (Lua script atomicity)
    const results = await Promise.allSettled(promises)
    const successes = results.filter(r => r.status === 'fulfilled')
    expect(successes).toHaveLength(1)
  })
  
  it('should expire records after TTL', async () => {
    const messageId = 'order-3'
    await store.markProcessing(messageId, { ttl: 1 })
    
    expect(await store.isDuplicate(messageId)).toBe(true)
    
    // Wait for TTL
    await sleep(1100)
    
    expect(await store.isDuplicate(messageId)).toBe(false)
  })
})

// Integration: Kafka consumer dedup works end-to-end
describe('DedupConsumer Integration', () => {
  it('should skip processing duplicate messages', async () => {
    const message = { id: 'order-1', value: { amount: 100 } }
    let processCount = 0
    
    const handler = async () => {
      processCount++
    }
    
    // Send same message twice
    await consumer.process(message, handler)
    await consumer.process(message, handler)
    
    // Should process only once
    expect(processCount).toBe(1)
  })
  
  it('should rollback on failure', async () => {
    const message = { id: 'order-2' }
    
    const handler = async () => {
      throw new Error('Database error')
    }
    
    const result = await consumer.process(message, handler)
    
    const status = await dedup.getStatus('order-2')
    expect(status).toEqual({
      status: 'FAILED',
      error: 'Database error',
      attempts: 1
    })
  })
})

// Chaos: Network failure during processing
describe('Chaos - Network Failures', () => {
  it('should handle Redis timeout gracefully', async () => {
    const redisProxy = new ToxiproxyRedis()
    redisProxy.toxics.add({
      type: 'timeout',
      stream: 'downstream',
      timeout: 100
    })
    
    const store = new RedisDeduplicationStore(redisProxy)
    
    // Should retry and eventually succeed or fail gracefully
    const result = await store.isDuplicate('msg-1')
    expect([true, false, Error]).toContain(result)
  })
  
  it('should handle Kafka broker unavailable', async () => {
    const consumer = new DedupConsumer({
      kafka: testKafka,
      retries: 3,
      retryDelay: 100
    })
    
    // Kill broker
    await kafka.stop()
    
    // Consumer should not crash, should retry
    const promise = consumer.start()
    await sleep(500)
    
    expect(consumer.status).toBe('RECONNECTING')
    
    // Restart broker
    await kafka.start()
    await promise
    
    expect(consumer.status).toBe('CONNECTED')
  })
})

// E2E: Full workflow with dedup
describe('E2E - Order Processing', () => {
  it('should handle order creation with duplicate messages', async () => {
    const kafka = await testKafka.createTopic('orders')
    const db = await testDB.init()
    const redis = await testRedis.init()
    
    const consumer = new DedupConsumer({
      topic: 'orders',
      dedup: new RedisDeduplicationStore(redis),
      handler: async (msg) => {
        return await createOrder(db, msg)
      }
    })
    
    await consumer.start()
    
    // Publish same order message 3 times (simulating retries)
    const orderMsg = {
      id: 'msg-123',
      value: { customerId: 'cust-1', amount: 100 }
    }
    
    await kafka.send(orderMsg)
    await kafka.send(orderMsg) // Duplicate
    await kafka.send(orderMsg) // Duplicate
    
    await sleep(2000)
    
    // Should create order only once
    const orders = await db('orders')
      .where('customer_id', 'cust-1')
    
    expect(orders).toHaveLength(1)
    expect(orders[0].amount).toBe(100)
  })
})
```

**Test Coverage Goals:**
- Core dedup logic: **100%**
- Consumer integration: **95%+**
- Error paths: **90%+**
- Overall: **90%+**

**Deliverables:**
- [ ] Test suite (500+ test cases)
- [ ] Coverage report (>90%)
- [ ] Chaos testing framework
- [ ] CI/CD test pipeline

**Claude's Role:**
- Generate test files
- Create test data factories
- Write chaos scenarios
- Build test utilities

---

### 4.2 Load Testing & Performance

**Objectives:**
- [ ] Benchmark dedup performance
- [ ] Load test at scale
- [ ] Identify bottlenecks

**Load Testing Scenarios:**

```yaml
scenarios:
  baseline:
    description: Normal load
    rps: 100
    dedup_hit_rate: 5%
    
  sustained_load:
    description: 24h sustained load
    rps: 1000
    dedup_hit_rate: 5%
    duration: 86400s
    
  spike:
    description: Traffic spike (10x normal)
    rps: 10000
    dedup_hit_rate: 15%
    duration: 300s
    
  duplicate_storm:
    description: High duplicate rate
    rps: 1000
    dedup_hit_rate: 50%
    duration: 60s

performance_targets:
  dedup_latency_p99: 5ms
  dedup_latency_p999: 10ms
  consumer_throughput: 10k msgs/sec
  redis_hit_rate: >95%
  false_negatives: 0
```

**K6 Load Test Script:**
```javascript
import http from 'k6/http'
import { check, sleep } from 'k6'
import { Rate, Trend, Counter } from 'k6/metrics'

const duplicateRate = new Rate('duplicate_detections')
const processingTime = new Trend('processing_time_ms')
const failureRate = new Rate('failures')

export let options = {
  stages: [
    { duration: '2m', target: 100 },
    { duration: '5m', target: 1000 },
    { duration: '2m', target: 0 }
  ],
  thresholds: {
    'duplicate_detections': ['rate > 0.05'],
    'processing_time_ms': ['p(99) < 10'],
    'failures': ['rate < 0.01']
  }
}

export default function() {
  const messageId = `msg-${__VU}-${Math.random()}`
  const payload = JSON.stringify({
    messageId,
    data: { amount: 100, customerId: 'cust-1' }
  })
  
  const res = http.post(
    'http://localhost:3000/process',
    payload,
    { headers: { 'Content-Type': 'application/json' } }
  )
  
  check(res, {
    'status is 200': (r) => r.status === 200,
    'processing < 10ms': (r) => r.timings.duration < 10
  })
  
  processingTime.add(res.timings.duration)
  
  if (res.body.isDuplicate) {
    duplicateRate.add(1)
  }
  
  sleep(1)
}
```

**Deliverables:**
- [ ] Load test scenarios
- [ ] K6 test scripts
- [ ] Performance baseline report
- [ ] Scaling recommendations

**Claude's Role:**
- Generate load test scripts
- Create performance baselines
- Suggest optimization strategies

---

## 🚀 Phase 5: Production Deployment & Operations (Weeks 9-10)

### 5.1 Deployment Pipeline

**Objectives:**
- [ ] CI/CD setup
- [ ] Deployment strategy
- [ ] Rollback procedures

**CI/CD Pipeline:**
```
GitHub / GitLab
    ↓
[Trigger on PR/merge]
    ↓
┌─────────────────────────────┐
│ CI Pipeline                 │
├─────────────────────────────┤
│ 1. Build Docker image       │
│ 2. Run unit tests (90%+)    │
│ 3. Run integration tests    │
│ 4. Lint & format check      │
│ 5. Security scan (SAST)     │
│ 6. Push to registry         │
└─────────────────────────────┘
    ↓ (only if all pass)
┌─────────────────────────────┐
│ Deploy to Staging           │
├─────────────────────────────┤
│ 1. Deploy consumer service  │
│ 2. Run smoke tests          │
│ 3. Run E2E tests            │
│ 4. Performance baseline     │
└─────────────────────────────┘
    ↓ (manual approval)
┌─────────────────────────────┐
│ Deploy to Production        │
├─────────────────────────────┤
│ 1. Blue-green deployment    │
│ 2. Gradual traffic shift    │
│ 3. Health checks            │
│ 4. Rollback on failure      │
└─────────────────────────────┘
```

**Deployment Configuration:**
```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dedup-consumer
  labels:
    app: dedup-consumer
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0  # Zero-downtime
  
  selector:
    matchLabels:
      app: dedup-consumer
  
  template:
    metadata:
      labels:
        app: dedup-consumer
    spec:
      containers:
      - name: consumer
        image: registry.example.com/dedup-consumer:v1.2.0
        
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        
        livenessProbe:
          httpGet:
            path: /health/live
            port: 3000
          initialDelaySeconds: 30
          periodSeconds: 10
        
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 3000
          initialDelaySeconds: 10
          periodSeconds: 5
        
        env:
        - name: REDIS_URL
          valueFrom:
            secretKeyRef:
              name: dedup-secrets
              key: redis-url
        - name: KAFKA_BROKERS
          valueFrom:
            configMapKeyRef:
              name: dedup-config
              key: kafka-brokers
```

**Rollback Strategy:**
```
Automated Rollback Triggers:
├── Error rate > 5% for 2 minutes
├── Latency p99 > 5s
├── Pod crash loop (>3 restarts)
├── Dedup service unavailable
└── Consumer lag > 1 hour

Manual Rollback:
├── kubectl rollout undo deployment/dedup-consumer
├── Verify previous version healthy
├── Post-mortem on what went wrong
└── Fix & redeploy
```

**Deliverables:**
- [ ] CI/CD pipeline configuration
- [ ] Kubernetes deployment manifests
- [ ] Rollback runbook
- [ ] Deployment checklist

**Claude's Role:**
- Generate CI/CD configs (GitHub Actions, GitLab CI)
- Create Kubernetes manifests
- Write deployment docs

---

### 5.2 Operational Runbooks

**Objectives:**
- [ ] Create runbooks for common issues
- [ ] Document troubleshooting procedures
- [ ] Build on-call playbooks

**Runbook Examples:**

```markdown
## Runbook: High Dedup Hit Rate (>10%)

### Problem
Alert: dedup_hit_rate > 0.1 for 5 minutes

### Investigation
1. Check recent Kafka offset resets:
   ```
   kubectl logs -l app=dedup-consumer | grep "offset reset"
   ```

2. Check for message replay:
   ```
   SELECT COUNT(*) FROM message_audit 
   WHERE created_at > now() - interval 5 minute
   GROUP BY message_id
   HAVING COUNT(*) > 1
   ```

3. Check Redis dedup store size:
   ```
   redis-cli INFO memory
   redis-cli DBSIZE
   ```

### Resolution
- **If offset reset:** Review broker logs, restart consumer
- **If message replay:** Contact engineering, may indicate upstream issue
- **If Redis full:** Increase TTL cleanup frequency, scale Redis

### Escalation
- If unresolved after 15min: Page on-call lead
- If business impact: Notify product


## Runbook: Consumer Lag > 1 Hour

### Problem
Alert: consumer_lag_seconds > 3600

### Investigation
1. Check consumer status:
   ```
   kubectl describe pod -l app=dedup-consumer
   kubectl logs -l app=dedup-consumer --tail=100
   ```

2. Check Redis performance:
   ```
   redis-cli --latency
   redis-cli --bigkeys
   ```

3. Check database performance:
   ```
   SELECT COUNT(*) FROM pg_stat_activity;
   EXPLAIN ANALYZE SELECT * FROM orders LIMIT 1;
   ```

### Resolution Options
- **Slow dedup lookups:** Scale Redis horizontally
- **Slow DB writes:** Optimize handler query, add indices
- **Consumer crash loop:** Check error logs, rollback if recent deploy
- **Backlog too large:** Increase consumer replicas

### Escalation
- If lag > 4 hours: Page on-call lead
- Business-critical: Consider manual message replay


## Runbook: Dedup Store Unavailable (Redis Down)

### Problem
Error: "Redis connection refused"

### Immediate Actions
1. Check Redis pod status:
   ```
   kubectl get pods -l app=redis
   kubectl describe pod <redis-pod>
   ```

2. Check Redis logs:
   ```
   kubectl logs <redis-pod>
   ```

3. If pod crashed, check events:
   ```
   kubectl get events -n default --sort-by='.lastTimestamp'
   ```

### Recovery Options
A. **Redis Pod Restart** (fast):
   ```
   kubectl delete pod <redis-pod>
   # Kubernetes auto-restarts
   ```

B. **Redis Failover** (if HA cluster):
   ```
   # Assuming Redis Sentinel
   kubectl exec -it redis-sentinel -- redis-cli sentinel failover
   ```

C. **Emergency Fallback** (disable dedup temporarily):
   ```
   kubectl set env deployment/dedup-consumer USE_DEDUP=false
   # Messages may be processed twice, acceptable for incident
   ```

### Restoration
- After Redis recovery: Set USE_DEDUP=true
- Monitor for duplicate processing (expected: 0-2%)
- Review metrics dashboard

### Escalation
- If Redis down > 5 min: Page infrastructure team
- If message duplication > 5%: Incident lead
```

**Deliverables:**
- [ ] 10+ detailed runbooks
- [ ] Troubleshooting decision tree
- [ ] Escalation procedures
- [ ] On-call playbook

**Claude's Role:**
- Generate runbooks
- Create decision trees
- Write escalation procedures

---

### 5.3 Monitoring & Alerting

**Objectives:**
- [ ] Set up production monitoring
- [ ] Configure alert rules
- [ ] Build incident response playbooks

**Alert Configuration:**
```yaml
# alerts.yaml
groups:
  - name: dedup.alerts
    interval: 30s
    rules:
      - alert: HighDuplicateDetectionRate
        expr: |
          rate(dedup_duplicates_detected_total[5m]) /
          rate(consumer_messages_processed_total[5m]) > 0.1
        for: 5m
        annotations:
          summary: "High dedup rate ({{ $value | humanizePercentage }})"
          description: "More than 10% of messages are duplicates"
          severity: "warning"
          runbook: "https://wiki/runbooks/high-dedup-rate"
      
      - alert: DeduplicationLatencyHigh
        expr: |
          histogram_quantile(0.99, 
            dedup_processing_time_seconds
          ) > 0.01
        for: 5m
        annotations:
          summary: "Dedup p99 latency > 10ms"
          description: "Deduplication is adding {{ $value }}s latency"
          severity: "critical"
      
      - alert: RedisDeduplicationStoreUnavailable
        expr: |
          up{job="redis-dedup"} == 0
        for: 1m
        annotations:
          summary: "Redis dedup store down"
          description: "Cannot connect to Redis"
          severity: "critical"
          action: "Investigate Redis pod, check logs"
      
      - alert: ConsumerLagTooHigh
        expr: |
          consumer_lag_seconds > 3600
        for: 10m
        annotations:
          summary: "Consumer lag > 1 hour"
          description: "Consumer {{ $labels.consumer }} is behind by {{ $value | humanizeDuration }}"
          severity: "critical"
      
      - alert: DLQMessagesAccumulating
        expr: |
          rate(dlq_messages_total[5m]) > 10
        for: 5m
        annotations:
          summary: "DLQ accumulating messages"
          description: "{{ $value | humanize }} messages/sec going to DLQ"
          severity: "warning"
          action: "Review DLQ messages, investigate root cause"
```

**Incident Response SLA:**
```
Severity  | Response Time | Resolution Time | Escalation
----------|---------------|-----------------|------------------
CRITICAL  | 5 min         | 30 min          | Page on-call lead
HIGH      | 15 min        | 2 hours         | Notify team lead
MEDIUM    | 1 hour        | 8 hours         | Create ticket
LOW       | 8 hours       | 1 week          | Backlog item
```

**Deliverables:**
- [ ] Prometheus alert rules
- [ ] Alertmanager configuration
- [ ] Incident response SLAs
- [ ] Alert routing (PagerDuty / Slack)

**Claude's Role:**
- Generate alert rules
- Create SLA documentation
- Write incident templates

---

## 📈 Phase 6: Optimization & Scale (Weeks 11-12)

### 6.1 Performance Optimization

**Objectives:**
- [ ] Optimize dedup lookup latency
- [ ] Improve throughput
- [ ] Reduce resource usage

**Optimization Opportunities:**

```typescript
// Optimization 1: Local cache + Redis cache
class TieredDeduplicationStore {
  private localCache = new LRU<string, boolean>({ max: 100000 })
  private redis = new Redis()
  
  async isDuplicate(messageId: string): Promise<boolean> {
    // L1: Check local cache (microseconds)
    if (this.localCache.has(messageId)) {
      return this.localCache.get(messageId)
    }
    
    // L2: Check Redis (milliseconds)
    const isDup = await this.redis.exists(messageId)
    
    if (isDup) {
      // Cache result locally
      this.localCache.set(messageId, true)
    }
    
    return isDup
  }
}

// Optimization 2: Batch operations
async function processBatch(messages: Message[]): Promise<void> {
  const messageIds = messages.map(m => m.id)
  
  // Single Redis call to check all
  const duplicates = await this.redis.mget(messageIds)
  
  const toProcess = messages.filter((msg, i) => !duplicates[i])
  
  // Process non-duplicates
  for (const msg of toProcess) {
    await handleMessage(msg)
  }
}

// Optimization 3: Async TTL cleanup
class BackgroundCleanup {
  async startCleanup(intervalMs = 60000): Promise<void> {
    setInterval(async () => {
      try {
        const deleted = await this.dedup.cleanupExpired(86400)
        metrics.recordCleanup(deleted)
      } catch (error) {
        logger.error('Cleanup failed', { error })
      }
    }, intervalMs)
  }
}

// Optimization 4: Connection pooling
const redisPool = new Pool({
  create: async () => new Redis(config.redis),
  destroy: async (client) => client.disconnect(),
  max: 50,
  min: 10
})
```

**Performance Targets After Optimization:**
```
Baseline vs Optimized:

Dedup Lookup Latency:
├── Before: p99 = 15ms, p999 = 50ms
└── After: p99 = 2ms, p999 = 10ms (10x improvement)

Throughput:
├── Before: 5k msgs/sec
└── After: 50k msgs/sec (10x improvement)

Redis Memory:
├── Before: 10GB
└── After: 2GB (with TTL cleanup) (5x improvement)

Consumer CPU:
├── Before: 8 cores @ 80%
└── After: 2 cores @ 40% (4x improvement)
```

**Deliverables:**
- [ ] Optimized dedup implementation
- [ ] Performance benchmark report
- [ ] Tuning guide (Redis, Kafka, DB)
- [ ] Scaling recommendations

**Claude's Role:**
- Suggest optimization strategies
- Generate optimized code
- Create performance reports

---

### 6.2 Scalability & Architecture Patterns

**Objectives:**
- [ ] Design for horizontal scaling
- [ ] Multi-region strategy
- [ ] Handle peak loads

**Scaling Strategies:**

```
Current Architecture (Single Region):
┌──────────────────────────┐
│  Kafka Cluster           │
│  (3+ brokers)            │
└──────────────┬───────────┘
               │
      ┌────────┼────────┐
      ▼        ▼        ▼
  Consumer Consumer Consumer  (Horizontal scaling)
  (Pod 1)  (Pod 2)  (Pod 3)
      │        │        │
      └────────┼────────┘
               │
      ┌────────┼────────┐
      ▼        ▼        ▼
   Redis Cluster      PostgreSQL
   (Dedup store)     (Audit logs)
   (Sentinel for HA)  (Read replicas)


Future: Multi-Region Architecture
┌─────────────────────────────────────┐
│  Region 1 (US-EAST)                 │
│  ┌─────────────────────────────┐   │
│  │ Kafka Cluster               │   │
│  │ Consumer Group              │   │
│  │ Dedup Store (Redis Cluster) │   │
│  │ Audit DB (PostgreSQL)       │   │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
           ↔ Replication
┌─────────────────────────────────────┐
│  Region 2 (EU-WEST)                 │
│  ┌─────────────────────────────┐   │
│  │ Kafka Cluster (Replica)     │   │
│  │ Consumer Group              │   │
│  │ Dedup Store (Redis Cluster) │   │
│  │ Audit DB (PostgreSQL)       │   │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
```

**Horizontal Scaling Checklist:**
```
Consumer Scaling (Kafka):
├── [x] Partition key strategy (by customerId, orderId, etc)
├── [x] Consumer group rebalancing
├── [x] Offset management (auto-commit disabled)
├── [x] Graceful shutdown (drain messages)
└── [x] Monitor partition lag per consumer

Redis Scaling (Dedup store):
├── [x] Redis Cluster (horizontal sharding)
├── [x] Sentinel setup (HA for single-node)
├── [x] Connection pooling
├── [x] Pipeline commands to reduce RTT
└── [x] Lua scripts for atomic ops

Database Scaling:
├── [x] Connection pooling
├── [x] Read replicas for audit queries
├── [x] Write to primary, read from replicas
├── [x] Index optimization
└── [x] Query optimization & caching

Monitoring Scaling:
├── [x] Metrics cardinality (labels)
├── [x] Log aggregation sharding
├── [x] Trace sampling (10% in production)
└── [x] Prometheus scrape interval tuning
```

**Deliverables:**
- [ ] Horizontal scaling guide
- [ ] Multi-region architecture doc
- [ ] Kubernetes HPA configuration
- [ ] Cost optimization report

**Claude's Role:**
- Generate scaling architectures
- Create HPA configs
- Write capacity planning docs

---

## 🎓 Phase 7: Documentation & Knowledge Transfer (Weeks 13-14)

### 7.1 Technical Documentation

**Objectives:**
- [ ] API documentation
- [ ] Architecture docs
- [ ] Developer guides

**Documentation Structure:**
```
docs/
├── README.md (overview, quick start)
├── ARCHITECTURE.md (system design, diagrams)
├── API.md (endpoints, request/response)
├── CONFIGURATION.md (env vars, settings)
├── DEPLOYMENT.md (staging, production)
├── TROUBLESHOOTING.md (common issues)
├── OPERATIONS.md (runbooks, alerts)
├── PERFORMANCE.md (tuning, benchmarks)
├── SCALING.md (horizontal scaling)
├── MONITORING.md (metrics, dashboards)
├── CONTRIBUTING.md (dev setup)
└── EXAMPLES/
    ├── basic_consumer.js
    ├── error_handling.js
    ├── custom_handler.js
    └── testing.js
```

**Sample API Documentation:**
```markdown
## API: Deduplication Service

### POST /deduplicate/check
Check if message ID has been processed.

**Request:**
```json
{
  "messageId": "order-123",
  "timeout": 5000
}
```

**Response:**
```json
{
  "isDuplicate": false,
  "status": "NEW",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Status Codes:**
- `200 OK`: Check successful
- `503 Service Unavailable`: Dedup store down (fallback: allow processing)
- `429 Too Many Requests`: Rate limited

### POST /deduplicate/mark-processing
Mark message as being processed.

...

### POST /deduplicate/mark-completed
Mark message as successfully completed.

...
```

**Deliverables:**
- [ ] API documentation (OpenAPI/Swagger)
- [ ] Architecture decision records (ADRs)
- [ ] Developer onboarding guide
- [ ] FAQ & troubleshooting guide

**Claude's Role:**
- Generate API docs
- Create architecture diagrams
- Write onboarding guides

---

### 7.2 Video Tutorials & Demos

**Objectives:**
- [ ] Record setup tutorial
- [ ] Demonstrate error scenarios
- [ ] Show monitoring dashboards

**Video Scripts:**

```markdown
## Video 1: Getting Started (5 min)
1. Prerequisites check
2. Clone repo
3. docker-compose up
4. Run example consumer
5. Send test messages
6. Verify dedup in action

## Video 2: Understanding Duplicates (8 min)
1. What is message duplication?
2. Real-world examples
3. Impact on business
4. Dedup architecture
5. Demo with failure injection

## Video 3: Monitoring & Alerts (6 min)
1. Access Grafana dashboard
2. Key metrics explained
3. Alert rule walkthrough
4. Responding to incident
5. Post-incident review

## Video 4: Troubleshooting (10 min)
1. High lag scenario
2. Redis unavailable scenario
3. Consumer crash loop
4. DLQ accumulation
5. Recovery steps
```

**Deliverables:**
- [ ] Setup tutorial video
- [ ] Architecture walkthrough
- [ ] Error scenario demos
- [ ] Monitoring dashboard tour

**Claude's Role:**
- Create video scripts
- Generate troubleshooting demos
- Build interactive tutorials

---

## 🏆 Phase 8: Advanced Features & Polish (Weeks 15-16)

### 8.1 Advanced Deduplication Patterns

**Objectives:**
- [ ] Implement advanced patterns
- [ ] Support edge cases
- [ ] Optimize for specific domains

**Advanced Patterns:**

```typescript
// Pattern 1: Windowed Deduplication
// Dedup only within a time window
class WindowedDeduplication {
  async isDuplicate(
    messageId: string,
    windowSize: number = 3600000 // 1 hour
  ): Promise<boolean> {
    const key = `dedup:${messageId}`
    const created = await redis.get(key)
    
    if (!created) return false
    
    const age = Date.now() - parseInt(created)
    return age < windowSize
  }
}

// Pattern 2: Probabilistic Deduplication
// Use Bloom filter for memory efficiency
import BloomFilter from 'bloom-filters'

class BloomFilterDeduplication {
  private filter = new BloomFilter.ScalableBloomFilter()
  
  isDuplicate(messageId: string): boolean {
    if (this.filter.has(messageId)) {
      // Possible duplicate (false positive rate ~1%)
      return await this.redis.exists(messageId)
    }
    this.filter.add(messageId)
    return false
  }
}

// Pattern 3: Cross-System Deduplication
// Dedup across multiple systems/databases
class CrossSystemDeduplication {
  async isDuplicate(messageId: string): Promise<boolean> {
    const checks = await Promise.all([
      this.redis.exists(messageId),
      this.db('processed_messages')
        .where('message_id', messageId).first(),
      this.audit.query(`SELECT * FROM audit WHERE message_id = ?`, messageId)
    ])
    
    return checks.some(result => result)
  }
}

// Pattern 4: Domain-Specific Idempotency
// For e-commerce: order dedup by (customerId, amount, itemIds)
class CompositeKeyDeduplication {
  async isDuplicate(
    customerId: string,
    amount: number,
    itemIds: string[]
  ): Promise<boolean> {
    const compositeKey = `order:${customerId}:${amount}:${itemIds.sort().join(',')}`
    return await this.redis.exists(compositeKey)
  }
}
```

**Deliverables:**
- [ ] Advanced dedup pattern library
- [ ] Use-case specific implementations
- [ ] Performance comparison

**Claude's Role:**
- Design advanced patterns
- Generate implementations
- Create pattern selection guide

---

### 8.2 Admin Dashboard & Tools

**Objectives:**
- [ ] Build admin UI
- [ ] Create operational tools
- [ ] Enable self-service debugging

**Admin Dashboard Features:**
```
Admin Dashboard Features:

1. Dedup Status Page
   ├── Real-time stats (hit rate, latency, throughput)
   ├── Search by message ID
   ├── View processing status
   ├── Manual replay capability
   └── Export metrics

2. Message Replay Tool
   ├── Query by message ID
   ├── Bulk replay
   ├── Replay with modifications
   ├── Dry-run execution
   └── Audit trail

3. Storage Management
   ├── TTL configuration
   ├── Manual cleanup trigger
   ├── Size monitoring
   ├── Compression options
   └── Backup/restore

4. Incident Response
   ├── View recent duplicates
   ├── Access logs by time range
   ├── Generate incident report
   ├── Export for analysis
   └── Notify teams

5. Configuration
   ├── Change dedup thresholds
   ├── Modify TTL
   ├── Update alert rules
   ├── Configure cleanup schedule
   └── Feature flags
```

**Admin API Endpoints:**
```
GET  /admin/dedup/stats              - Current statistics
GET  /admin/dedup/message/{id}       - Get message status
POST /admin/dedup/cleanup            - Trigger cleanup
POST /admin/message/replay/{id}      - Replay single message
POST /admin/message/replay-bulk      - Replay multiple messages
GET  /admin/logs                      - Query logs
POST /admin/incident/report           - Generate incident report
GET  /admin/config                    - Get current config
POST /admin/config                    - Update config
```

**Deliverables:**
- [ ] Admin dashboard React app
- [ ] Admin API endpoints
- [ ] Authorization & RBAC
- [ ] Audit logging for admin actions

**Claude's Role:**
- Generate dashboard UI
- Create admin API handlers
- Build authorization layer

---

## 📊 Success Metrics & KPIs

**Define Success:**
```
Technical Metrics:
├── Dedup Latency (p99): < 5ms ✅
├── False Negative Rate: 0% ✅
├── System Uptime: > 99.95% ✅
├── Mean Recovery Time: < 15min ✅
└── Cost per dedup: < $0.00001 ✅

Business Metrics:
├── Duplicate Transactions Prevented: 99.9% ✅
├── Revenue Saved (estimated): > $1M/year ✅
├── Customer Complaints (duplicates): 0 ✅
├── Data Inconsistency Incidents: 0 ✅
└── Engineer Time Saved: > 100 hrs/year ✅

Quality Metrics:
├── Code Coverage: > 90% ✅
├── Test Pass Rate: 100% ✅
├── Documentation Coverage: 100% ✅
├── Runbook Coverage: 95%+ ✅
└── No Critical Bugs in Prod: Yes ✅
```

---

## 🎓 Learning Outcomes

After completing this project, you'll understand:

**Distributed Systems:**
- [ ] Eventual consistency vs strong consistency
- [ ] CAP theorem and trade-offs
- [ ] Message ordering guarantees
- [ ] Network failures and retries

**Data & Databases:**
- [ ] ACID transactions
- [ ] Unique constraints for idempotency
- [ ] Redis data structures
- [ ] TTL and expiration

**Kafka & Event Streaming:**
- [ ] Consumer groups and offsets
- [ ] Rebalancing logic
- [ ] Exactly-once vs at-least-once semantics
- [ ] Retry and DLQ patterns

**Observability & Operations:**
- [ ] Prometheus metrics design
- [ ] Distributed tracing
- [ ] Alert design and escalation
- [ ] On-call playbooks and runbooks

**Production Engineering:**
- [ ] CI/CD pipelines
- [ ] Blue-green deployments
- [ ] Kubernetes stateless services
- [ ] Chaos engineering

**Soft Skills:**
- [ ] Clear technical documentation
- [ ] Incident response
- [ ] Cross-team communication
- [ ] On-call mentality

---

## 📚 Resources & References

**Key Papers & Concepts:**
- [ ] "Exactly-Once Semantics in Apache Kafka" - Confluent
- [ ] "Designing Data-Intensive Applications" - Martin Kleppmann (Chapter 11)
- [ ] "The Outbox Pattern" - Chris Richardson
- [ ] "Idempotency in Practice" - Will Larson

**Tools & Libraries:**
```
Kafka: Confluent Kafka JS / Apache Kafka
Redis: redis, redis-sentinel, redis-cluster
Database: PostgreSQL, MongoDB
Testing: Jest, Toxiproxy, Testcontainers
Monitoring: Prometheus, Grafana, Loki
Tracing: Jaeger, OpenTelemetry
Container: Docker, Kubernetes
```

**Community & Learning:**
- [ ] Kafka Summit talks
- [ ] Confluent blog posts
- [ ] Redis documentation
- [ ] Kubernetes in Action book

---

## 🚦 Phase Timeline Summary

```
Week 1-2   : Foundation & Architecture        ████░░░░░░░░░░░░
Week 3-4   : Consumer Integration              ░░░░████░░░░░░░░
Week 5-6   : Monitoring & Dashboards           ░░░░░░░░████░░░░
Week 7-8   : Testing & Resilience              ░░░░░░░░░░░░████
Week 9-10  : Deployment & Operations           ░░░░░░░░░░░░░░░░░░░░████
Week 11-12 : Optimization & Scaling            ░░░░░░░░░░░░░░░░░░░░░░░░████
Week 13-14 : Documentation & Knowledge Transfer ░░░░░░░░░░░░░░░░░░░░░░░░░░░░████
Week 15-16 : Advanced Features & Polish        ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████

Total: 16 weeks (4 months) for complete implementation
Minimum MVP: 4 weeks (Phase 1-2 + basic testing)
```

---

## 🎯 Next Steps

1. **Choose Your Tech Stack:** Start with Phase 1.1
2. **Set Up Development Environment:** Follow Phase 1.2
3. **Build Core Dedup Layer:** Complete Phase 1.3
4. **Integrate with Kafka:** Work through Phase 2.1-2.2
5. **Add Monitoring:** Implement Phase 3.1-3.3
6. **Rigorous Testing:** Execute Phase 4
7. **Deploy & Iterate:** Follow Phase 5-8

---

**Good luck building! This is a production-grade system that will serve as a portfolio piece demonstrating your understanding of distributed systems, event-driven architecture, and operational excellence.**

**Questions?** Reference the phase docs or check the community resources.
