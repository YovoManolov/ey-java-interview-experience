# EY Technical Interview Prep — Java / Spring Boot / Microservices / AWS / Kafka

> **Source:** The questions below come from **Anubhi Sharma** (Senior Software Engineer at Cognizant), who shared them on 2026-09-26 after her first technical interview round at EY. The questions are hers; the answers are prep notes written for them.

---

## Java

### 1. Difference between HashMap and ConcurrentHashMap.
| | HashMap | ConcurrentHashMap |
|---|---|---|
| Thread safety | None | Yes, without locking the whole map |
| Locking | N/A | Java 8+: CAS + synchronized on the bin's first node (fine-grained, per-bucket) |
| Null keys/values | 1 null key, multiple null values allowed | No null keys or values (avoids ambiguity between "absent" and "null" under concurrent access) |
| Iterators | Fail-fast (`ConcurrentModificationException`) | Weakly consistent — won't throw CME, may or may not reflect concurrent updates |
| Performance | Faster single-threaded | Slight overhead, but scales under contention |

Pre-Java 8, `ConcurrentHashMap` used segment-level locking (16 segments). Since Java 8 it dropped segments and locks per-bin (a `Node` linked list or tree), with `size()` counters implemented via `LongAdder`-style striped counting to avoid contention.

### 2. Explain Java Memory Management and different JVM memory areas.
- **Heap** — shared across threads, holds objects and arrays.
  - **Young Gen**: Eden + 2 Survivor spaces (S0/S1). New objects allocated in Eden; minor GC copies survivors between S0/S1; objects surviving enough cycles get promoted.
  - **Old Gen (Tenured)**: long-lived objects; collected by major/full GC (more expensive).
- **Method Area / Metaspace** — class metadata, static variables, constant pool. Since Java 8, Metaspace replaced PermGen and lives in native memory (not bounded by `-Xmx`, controlled via `-XX:MaxMetaspaceSize`).
- **Stack** — one per thread; each method call pushes a stack frame containing local variables, operand stack, and frame data. Deallocated automatically when the method returns; `StackOverflowError` on excessive depth (e.g., unbounded recursion).
- **PC Register** — one per thread (see Q3).
- **Native Method Stack** — supports native (JNI) method calls.

### 3. What is the PC Register?
Each thread has its own Program Counter register, holding the address of the JVM instruction currently being executed. If the method is native, the PC register value is undefined. It's what lets the JVM resume the correct instruction after a context switch between threads.

### 4. Explain String Pool vs Heap memory.
The String Pool (a.k.a. intern pool) is a special region — since Java 7 it lives inside the heap (previously in PermGen) — that stores unique String literals so identical literals can be reused instead of duplicated. Regular objects, including `String` objects created via `new`, live on the general heap outside the pool. The pool exists purely to save memory for the extremely common case of repeated string literals.

### 5. What happens when we use `new String("hello")` if `"hello"` already exists in the String Pool?
Two objects end up existing:
1. The pooled `"hello"` literal (created at class-load/first-reference time, one instance ever).
2. A brand-new `String` object on the heap created by `new`, with its own reference, even though its content is identical.

The heap object is **not** automatically pool-deduplicated. You can force the pool reference back with `.intern()`, e.g. `String s = new String("hello").intern();` — `s == "hello"` (the literal) becomes `true`.

### 6. Difference between `==` and `.equals()` for String.
- `==` compares references (memory addresses) — true only if both references point to the same object.
- `.equals()` (overridden in `String`) compares character content.

Two literals (`"hello" == "hello"`) are `true` because both are pulled from the pool. `new String("hello") == "hello"` is `false` because `new` always allocates a fresh heap object.

### 7. Difference between ExecutorService and CompletableFuture.
- **ExecutorService**: manages a thread pool; you submit `Runnable`/`Callable` and get back a `Future`. `Future.get()` is **blocking** and offers no composition — you can't chain "when this finishes, do that" without manually blocking.
- **CompletableFuture**: a richer async abstraction supporting non-blocking composition — `thenApply`, `thenCompose` (flatMap-style chaining of async ops), `thenCombine` (combine two independent futures), exception handling via `exceptionally`/`handle`, and manual completion (`complete()`). It implements both `Future` and `CompletionStage`.

In short: `ExecutorService` is the execution engine; `CompletableFuture` is the composition/orchestration layer that can run on top of an executor.

### 8. Does CompletableFuture create its own thread pool? Which pool does it use by default?
No — by default, the no-executor-argument methods (`supplyAsync`, `thenApplyAsync` without an `Executor`) run on the JVM-wide **`ForkJoinPool.commonPool()`**, sized to `Runtime.availableProcessors() - 1` by default. This is a common production pitfall: blocking I/O work submitted without an explicit `Executor` starves the shared common pool used by parallel streams and other `CompletableFuture` chains elsewhere in the JVM. Best practice: always pass a dedicated `Executor` for I/O-bound async work.

---

## System Design / Microservices

### 9. Design a fund transfer system capable of handling millions of transactions.
Key building blocks:
- **API Gateway** → **Transfer Service** (orchestrates the transfer, stateless, horizontally scalable).
- **Ledger/Account Service** owning account balances, backed by a relational store (strong consistency for money).
- **Idempotency layer**: every transfer request carries a client-generated idempotency key, deduplicated at the DB with a unique constraint.
- **Async processing via Kafka**: transfer requests published as events, partitioned by account ID for ordering; downstream consumers (fraud check, notification, audit) subscribe independently.
- **Outbox pattern** to guarantee "DB write + event publish" atomicity (no dual-write problem).
- **Saga pattern** to coordinate the debit/credit across services with compensation on failure (see Q15/16).
- **Horizontal scaling**: stateless services behind a load balancer, DB read replicas for balance queries, sharding by account ID range/hash if a single DB becomes the bottleneck.
- **Idempotent, at-least-once consumers** with dedupe tables, since Kafka's default delivery semantics are at-least-once.
- Observability: distributed tracing (correlation ID per transfer), metrics, and a reconciliation job that periodically verifies ledger consistency.

### 10. What happens if Account A is debited but Account B's credit fails?
This is exactly the distributed-transaction problem two-phase commit is bad at solving across microservice boundaries (blocking, poor availability). Instead:
- Treat it as a **Saga**: the debit is a local, committed transaction. If the subsequent credit step fails (transient or permanent), the orchestrator triggers a **compensating transaction** — credit A back the same amount.
- Retries with exponential backoff should be attempted first for transient failures (e.g., B's service momentarily down) before compensating.
- The whole transfer's state (`PENDING → DEBITED → CREDITED/COMPENSATED → FAILED`) is persisted so a crash mid-flow can be resumed/reconciled rather than losing money.

### 11. How would you prevent duplicate transactions if the user clicks multiple times?
- Client generates a unique **idempotency key** (UUID) per transfer attempt, sent in the request.
- Server persists `(idempotency_key)` with a DB **unique constraint**; a duplicate request with the same key either returns the cached result of the original processing or is rejected outright — it's never reprocessed.
- Frontend disables the submit button after the first click as a UX safeguard, but the server-side idempotency check is the actual guarantee.

### 12. How would you ensure transactions for an account are processed in order?
- If using Kafka: key every message by `accountId` so all events for that account land on the **same partition**, and a single consumer thread processes that partition sequentially (Kafka only guarantees order within a partition, never across partitions).
- If using a DB directly: use optimistic concurrency (a `version`/sequence column on the account row) or row-level locking (`SELECT ... FOR UPDATE`) so concurrent transfers touching the same account serialize correctly.

### 13. How would Kafka partitions help with ordering?
Kafka guarantees strict ordering **only within a partition**. By hashing on `accountId` as the partition key, every event for a given account always routes to the same partition and is consumed in the order it was produced. Cross-account ordering isn't guaranteed or needed — accounts are independent, so ordering only matters per-account, which the partitioning scheme delivers naturally while still parallelizing across many accounts/partitions.

### 14. Which design would you use to avoid duplicate transactions and ensure correct processing?
Combine:
- Idempotency keys at the API layer (Q11).
- Kafka topic partitioned by account ID (Q13) for per-account ordering.
- Idempotent consumers — a processed-events dedupe table keyed by (idempotency key or Kafka offset) since Kafka delivery is at-least-once by default.
- Saga orchestration with compensations for cross-service consistency (Q15).
- Optionally, Kafka transactions / exactly-once semantics (`enable.idempotence=true`, transactional producer) if you need to atomically produce to multiple topics/partitions.

### 15. Explain how the Saga pattern can be used for fund transfer.
Saga breaks a distributed transaction into a sequence of local transactions, each with a **compensating action** if a later step fails. Two styles:
- **Choreography**: each service publishes an event on completion; the next service reacts. Simple but hard to trace/debug at scale.
- **Orchestration** (preferred for money movement): a central orchestrator explicitly invokes each step and decides what compensation to run on failure — much clearer audit trail, which matters for financial correctness.

For a transfer: Step 1 debit A (local tx) → Step 2 credit B (local tx). If Step 2 fails, run the compensation for Step 1 (credit A back).

### 16. Explain the architecture/flow of a Saga-based fund transfer system.
1. **Transfer Saga Orchestrator** receives the transfer request, creates a saga instance with state `STARTED`, persisted in a saga-state table (or event store).
2. Orchestrator calls **Debit Service** → local DB transaction debits A → emits `DebitCompleted` (or `DebitFailed`).
3. On `DebitCompleted`, orchestrator calls **Credit Service** → local DB transaction credits B → emits `CreditCompleted` (or `CreditFailed`).
4. On `CreditCompleted`, orchestrator marks saga `COMPLETED`, emits `TransferCompleted` for downstream consumers (notifications, audit log).
5. On `CreditFailed`, orchestrator invokes **Compensate-Debit** (refund A) → emits `TransferFailed`.
6. A timeout/watchdog process scans for sagas stuck mid-flight (e.g., a service never responded) and either retries or forces compensation.
Communication is via Kafka topics per event type; the orchestrator itself can be implemented as a Kafka Streams app or a plain service consuming/producing events, with saga state in a relational table for auditability.

### 17. Which database would you choose and why?
A relational database (PostgreSQL or Oracle, depending on the shop) for the core ledger — money movement needs ACID guarantees (atomicity of debit/credit within a local transaction, durability, strong consistency for balance reads). NoSQL stores trade away exactly the consistency guarantees financial ledgers need. Scale is handled via read replicas for balance queries, connection pooling, and if write throughput genuinely outgrows a single instance, sharding by account ID range/hash — not by abandoning ACID semantics.

### 18. How do microservices communicate with each other?
- **Synchronous**: REST or gRPC, for request/response where the caller needs an immediate answer (e.g., "get current balance").
- **Asynchronous**: message brokers like Kafka or RabbitMQ, for event-driven, decoupled communication where the producer doesn't need to wait for consumers.
- **Service mesh** (Istio/Linkerd) is often layered on top for cross-cutting concerns: mTLS, retries, circuit breaking, observability — independent of the app code.

### 19. When would you use REST vs Kafka?
- **REST**: caller needs a synchronous result now, simple point-to-point interactions, low fan-out (one consumer).
- **Kafka**: you need decoupling between producer and consumer, multiple independent consumers of the same event, durability/replay of the event stream, or high throughput where blocking on a synchronous response would be a bottleneck. Fund transfers typically use REST for the client-facing "initiate transfer" call, then Kafka internally for the async orchestration/side-effects.

### 20. How would you handle cascading failures in microservices?
- **Circuit breakers** (Resilience4j) — stop calling a failing downstream after a threshold, fail fast instead of piling up threads.
- **Bulkheads** — isolate thread/connection pools per downstream dependency so one slow dependency can't exhaust resources needed by others.
- **Timeouts** on every network call (never rely on defaults).
- **Retries with exponential backoff + jitter**, capped, and only for idempotent operations.
- **Rate limiting / load shedding** at the edge to protect core services during overload.
- **Fallbacks** — degrade gracefully (cached/stale data, default response) rather than failing the whole chain.

### 21. How would you process 1 lakh (100,000) messages/second in Kafka?
- **Partition count**: enough partitions to parallelize across consumer instances — partitions are the unit of parallelism.
- **Producer tuning**: increase `batch.size` and `linger.ms` to batch more records per request; enable compression (`lz4` or `snappy`) to cut network/disk I/O.
- **Durability trade-off**: `acks=1` for higher throughput vs `acks=all` (with `min.insync.replicas`) for durability — choose based on whether financial correctness outweighs raw throughput (for money, usually `acks=all`).
- **Broker sizing**: enough brokers and disks (often NVMe) to sustain the aggregate write throughput; appropriate replication factor (commonly 3) balanced against write amplification.
- **Consumer side**: scale consumer group instances up to the partition count, tune `fetch.min.bytes`/`max.poll.records` for batch consumption, and avoid slow per-message synchronous downstream calls in the consumer loop.
- **Key distribution**: ensure the partition key doesn't create a hot partition (e.g., a single dominant account ID).

---

## Spring

### 22. Explain the flow of Spring MVC from request to response.
1. Request hits **DispatcherServlet** (the front controller — single entry point).
2. DispatcherServlet consults **HandlerMapping** to find which controller/method matches the URL + HTTP method.
3. **HandlerAdapter** invokes the matched controller method (after any `HandlerInterceptor.preHandle`).
4. Controller executes business logic (typically delegating to a service layer), returns either a `ModelAndView` (traditional MVC with a view) or, for REST APIs, an object annotated for the response body.
5. For REST controllers, an **HttpMessageConverter** (e.g., Jackson) serializes the returned object to JSON/XML directly onto the response.
6. For view-based MVC, the **ViewResolver** resolves the logical view name to an actual view (JSP/Thymeleaf), which renders using the model data.
7. `HandlerInterceptor.postHandle`/`afterCompletion` run, then the response is written back to the client.
Filters (Servlet-level, e.g., security, logging) wrap the whole DispatcherServlet invocation; interceptors (Spring-level) wrap the controller invocation specifically.

### 23. What is the N+1 problem in Hibernate/JPA and how would you solve it?
Occurs when fetching a list of N parent entities (1 query), then lazily accessing a related collection/association on each one individually triggers N additional queries — 1 + N total instead of a handful.

Fixes:
- **`JOIN FETCH`** in JPQL/HQL to eagerly load the association in the same query.
- **`@EntityGraph`** to declaratively specify which associations to fetch eagerly for a given query, without changing the entity's default fetch type globally.
- **Batch fetching** (`hibernate.default_batch_fetch_size` or `@BatchSize`) — Hibernate fetches associations for several parent entities in one `IN (...)` query instead of one query per parent.
- **DTO projections** — select only the needed columns/joins directly, bypassing entity graph traversal entirely; often the best option for read-heavy endpoints.

---

## Security

### 24. Difference/relationship between OAuth 2.0, JWT and OpenID Connect.
- **OAuth 2.0** is an **authorization** framework — it defines how a client can obtain an access token to act on a user's behalf against a resource server (answers "what can this client access?"). It doesn't mandate any particular token format.
- **JWT** is a **token format** — a compact, self-contained, signed (and optionally encrypted) structure for carrying claims. It's commonly used to represent OAuth2 access tokens or OIDC ID tokens, but JWT itself is unrelated to authorization/authentication protocols — it's just an encoding.
- **OpenID Connect (OIDC)** is an **authentication** layer built on top of OAuth 2.0 — it standardizes how a client verifies user identity, introducing the **ID Token** (always a JWT, containing identity claims like `sub`, `email`) and a standard `/userinfo` endpoint (answers "who is this user?").

In short: OAuth2 = delegated authorization protocol, OIDC = identity/authentication protocol layered on OAuth2, JWT = the token encoding format both often use.

### 25. Explain JWT structure and JWT authentication/validation flow.
Structure — three base64url-encoded parts joined by dots: `header.payload.signature`
- **Header**: algorithm (`alg`, e.g. `RS256`/`HS256`) and token type (`typ: JWT`).
- **Payload**: claims — registered (`iss`, `sub`, `exp`, `iat`, `aud`), and custom (roles, permissions).
- **Signature**: `HMACSHA256(base64(header)+"."+base64(payload), secret)` for symmetric algorithms, or an RSA/EC private-key signature for asymmetric — this is what makes the token tamper-evident.

Flow:
1. Client authenticates against the Authorization Server (credentials, login form, etc.).
2. Auth server issues a signed JWT (access token, and for OIDC an ID token).
3. Client stores the token and sends it as `Authorization: Bearer <token>` on subsequent API calls.
4. Resource server validates the signature using the shared secret (HMAC) or the issuer's public key (RSA/EC, often fetched via a JWKS endpoint), checks `exp`/`nbf`/`iss`/`aud` claims.
5. If valid, the server trusts the claims (no DB/session lookup needed — this is what makes JWT auth stateless) and authorizes the request based on embedded roles/scopes.

---

## AWS

### 26. Difference between EC2, ECS and Lambda.
- **EC2**: raw virtual machines — full control over the OS, you manage patching, scaling, and the runtime. Most flexible, most operational overhead.
- **ECS**: container orchestration — schedules and scales Docker containers, either on EC2 instances you manage (EC2 launch type) or on **Fargate** (serverless, AWS manages the underlying compute). You define task definitions/services; ECS handles placement, health checks, and scaling.
- **Lambda**: fully serverless functions-as-a-service — no server management at all, event-driven (API Gateway, S3, SQS, etc. trigger invocations), billed per invocation + duration, subject to execution time limits (15 min max) and cold-start latency. Best for short-lived, event-driven, spiky workloads; less ideal for long-running or highly stateful services.

### 27. How do you monitor applications in AWS?
- **CloudWatch**: metrics (CPU, memory, custom app metrics via the CloudWatch agent/SDK), Logs (centralized log aggregation from ECS/Lambda/EC2), Alarms (trigger notifications/auto-scaling on thresholds), Dashboards.
- **X-Ray**: distributed tracing across microservice calls — critical for pinpointing latency in a call chain.
- **CloudTrail**: audit log of AWS API calls (who did what, for compliance/security investigation).
- **ALB/Target Group health checks** and **ECS/EKS container health checks** for service-level liveness.
- Many shops layer **Prometheus + Grafana** on top of/alongside CloudWatch for more flexible dashboards and alerting.

### 28. Have you deployed microservices on AWS? Explain the deployment flow.
1. CI pipeline (GitHub Actions/Jenkins) builds the application, runs tests, builds a **Docker image**, and pushes it to **ECR**.
2. Pipeline triggers a deployment: update the **ECS task definition** with the new image tag, then update the **ECS service** (or trigger a rolling update on **EKS**).
3. Deployment strategy — rolling update or blue/green (via CodeDeploy) — shifts traffic to the new version gradually while the **ALB** health checks gate promotion; automatic rollback on failed health checks.
4. Configuration/secrets pulled from **Parameter Store/Secrets Manager** at container startup, not baked into the image.
5. Post-deploy, **CloudWatch alarms** watch error rates/latency; a spike triggers rollback or paging.

---

## Database & Troubleshooting

### 29. How would you identify and resolve a database bottleneck?
1. Check slow query logs and run `EXPLAIN`/`EXPLAIN ANALYZE` on suspect queries to see the execution plan (full table scans, missing index usage).
2. Look for missing or unused indexes; also check for N+1 query patterns from the application layer.
3. Check for **lock contention** (long-running transactions holding row/table locks).
4. Check infrastructure metrics — CPU, disk I/O, connection pool saturation (are you hitting `max_connections`?).
5. Fixes, roughly in order of effort: add/adjust indexes → optimize the query/schema → add caching (Redis) for hot reads → add read replicas to offload read traffic → as a last resort, shard/partition the data if write throughput itself is the ceiling.

### 30. How would you debug a 401 Unauthorized error?
- Confirm the request actually includes the `Authorization` header/token.
- Decode the JWT (if used) and check `exp` (expired?), `nbf`, `iss`, `aud` — a common cause is an expired token or audience/issuer mismatch.
- Check for **clock skew** between the token issuer and the validating server.
- Verify the signing key hasn't rotated without the resource server refreshing its JWKS cache.
- Check the `WWW-Authenticate` response header — it often states the reason (`invalid_token`, `expired_token`).
- In Spring Security specifically, check the security filter chain order — a misconfigured filter can reject valid tokens before they reach the intended handler.

### 31. How would you debug a 404 Not Found error?
- Verify the exact URL and HTTP method match a mapping that actually exists (typos, missing context path, trailing slash mismatches).
- Check API Gateway/reverse proxy routing rules and any path-rewriting configuration — a very common cause in microservices where the gateway strips or adds a prefix.
- Confirm the resource identified by the path variable actually exists in the data store (a "correct route, missing entity" 404 vs a "no such route" 404 — worth distinguishing in logs/response body).
- Check server-side routing/controller logs to see whether the request even reached the intended service, or was dropped/misrouted upstream (DNS, load balancer target group misconfiguration).
- Check API versioning — hitting `/v1/...` when the endpoint only exists under `/v2/...`.

---

## Coding Question

### 32. Given a list of account transactions such as `A:+100, B:+200, A:-50, C:+300, B:-100, A:+150`, calculate the final balance for each account.

#### Explain the approach/logic.
Single pass over the transaction list, accumulating a running total per account key in a hash map. Each transaction only needs to update one entry — no need to group first and sum afterward, `Map.merge()` does both atomically per entry: if the key is absent, insert the value; if present, combine it with the existing value using the given function (`Integer::sum` here).

#### Write the code.

```java
import java.util.*;
import java.util.regex.*;

public class AccountBalanceCalculator {

    // Represents one parsed transaction, e.g. "A:+100" -> ("A", 100)
    record Transaction(String account, int amount) {}

    public static Map<String, Integer> calculateBalances(List<Transaction> transactions) {
        Map<String, Integer> balances = new LinkedHashMap<>(); // preserves first-seen order
        for (Transaction t : transactions) {
            balances.merge(t.account(), t.amount(), Integer::sum);
        }
        return balances;
    }

    // Optional: parse raw "A:+100" style strings into Transaction objects
    public static Transaction parse(String raw) {
        String[] parts = raw.split(":");
        return new Transaction(parts[0], Integer.parseInt(parts[1]));
    }

    public static void main(String[] args) {
        List<String> raw = List.of("A:+100", "B:+200", "A:-50", "C:+300", "B:-100", "A:+150");

        Map<String, Integer> balances = raw.stream()
                .map(AccountBalanceCalculator::parse)
                .collect(
                        LinkedHashMap::new,
                        (map, t) -> map.merge(t.account(), t.amount(), Integer::sum),
                        Map::putAll
                );

        balances.forEach((account, balance) -> System.out.println(account + ": " + balance));
    }
}
```

**Output:**
```
A: 200
B: 100
C: 300
```

#### What is the time complexity?
**O(n)** — one pass over the n transactions; each `merge()` call is O(1) average (amortized hash map insert/update).

#### What is the space complexity?
**O(k)**, where k = number of distinct accounts (worst case O(n) if every transaction is for a different account).

#### Can you solve it using Java 8 features such as `Map.merge()`?
`Map.merge(key, value, remappingFunction)` collapses the classic "check if present, then either put or update" pattern into a single atomic call — no manual `containsKey`/`get`/`put` branching needed. It's the idiomatic Java 8+ way to solve exactly this class of "group and aggregate" problem, and pairs naturally with a `Stream` pipeline if the input is being transformed (parsed, filtered) on the way in, as shown in `main()`.
