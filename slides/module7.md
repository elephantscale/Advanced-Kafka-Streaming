# Module 7 — Security & Governance

Elephant Scale

---

## Module 7 Agenda

- TLS and SASL_SSL configuration
- Authentication mechanisms: Kerberos, SCRAM, OAuth, RBAC
- Authorization via ACLs
- Encryption and secrets management
- Governance best practices
- Security posture for enterprise Kafka deployments
- Compliance considerations

---

## Why Kafka Security Matters

An unsecured Kafka cluster:
- Any client can read **any topic** (including sensitive PII/financial data)
- Any client can **write to any topic** (data poisoning)
- Any client can **delete topics** or alter configurations
- All data in transit is **plaintext** (interceptable)
- No audit trail of who accessed what

> By default, Kafka has **no authentication, no authorization, no encryption**.

---

## Kafka Security Layers

```
┌──────────────────────────────────────┐
│  Encryption (TLS)                    │  ← data in transit
│  Authentication (SASL/mTLS)          │  ← who are you?
│  Authorization (ACLs / RBAC)         │  ← what can you do?
│  Audit Logging                       │  ← what did you do?
│  Encryption at Rest (OS/disk level)  │  ← data at rest
└──────────────────────────────────────┘
```

---

## TLS Configuration — Broker

```properties
# server.properties
listeners=SASL_SSL://0.0.0.0:9093
advertised.listeners=SASL_SSL://broker1.example.com:9093

ssl.keystore.location=/etc/kafka/ssl/kafka.server.keystore.jks
ssl.keystore.password=${env:KEYSTORE_PASSWORD}
ssl.key.password=${env:KEY_PASSWORD}
ssl.truststore.location=/etc/kafka/ssl/kafka.server.truststore.jks
ssl.truststore.password=${env:TRUSTSTORE_PASSWORD}

ssl.client.auth=required        # require client certificates (mTLS)
ssl.protocol=TLSv1.3
ssl.enabled.protocols=TLSv1.3,TLSv1.2
```

---

## TLS Configuration — Client

```properties
# producer/consumer
security.protocol=SASL_SSL
ssl.truststore.location=/etc/kafka/ssl/client.truststore.jks
ssl.truststore.password=changeit

# For mTLS (mutual TLS):
ssl.keystore.location=/etc/kafka/ssl/client.keystore.jks
ssl.keystore.password=changeit
```

```python
# Python (confluent-kafka)
conf = {
    'bootstrap.servers': 'kafka:9093',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': '/etc/kafka/ssl/ca.pem',
    'ssl.certificate.location': '/etc/kafka/ssl/client.pem',
    'ssl.key.location': '/etc/kafka/ssl/client.key',
}
```

---

## Authentication Mechanisms

 Mechanism  Description  Use When
---------
 SASL/PLAIN  Username + password (TLS required)  Dev/test, simple deployments
 SASL/SCRAM-SHA-256/512  Salted challenge-response  Production without Kerberos
 SASL/GSSAPI (Kerberos)  Enterprise Kerberos tickets  Enterprise with Active Directory
 SASL/OAUTHBEARER  OAuth 2.0 / OIDC tokens  Cloud-native, microservices
 mTLS  Client certificates  High-assurance, zero-trust

---

## SASL/SCRAM Configuration

Create user credentials (stored in ZooKeeper / KRaft metadata):
```bash
kafka-configs.sh \
  --bootstrap-server kafka:9092 \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=mysecretpassword]' \
  --entity-type users \
  --entity-name alice
```

Broker config:
```properties
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
```

Client config:
```properties
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="alice" password="mysecretpassword";
```

---

## OAuth / OIDC Authentication

Modern cloud-native deployments use OAuth 2.0:

```properties
# Broker
sasl.enabled.mechanisms=OAUTHBEARER
sasl.oauthbearer.token.endpoint.url=https://idp.example.com/oauth/token
sasl.oauthbearer.expected.audience=kafka-cluster
listener.name.sasl_ssl.oauthbearer.sasl.server.callback.handler.class=\
  org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler
```

```properties
# Client
sasl.mechanism=OAUTHBEARER
sasl.oauthbearer.token.endpoint.url=https://idp.example.com/oauth/token
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
  clientId="my-service" clientSecret="..." scope="kafka-read";
```

---

## Authorization — ACLs

Kafka ACLs control what authenticated principals can do:

```bash
# Allow alice to produce to orders topic
kafka-acls.sh \
  --bootstrap-server kafka:9093 \
  --command-config /etc/kafka/admin.properties \
  --add \
  --allow-principal User:alice \
  --operation Write \
  --topic orders

# Allow payment-service to consume from orders
kafka-acls.sh \
  --add \
  --allow-principal User:payment-service \
  --operation Read \
  --topic orders \
  --group payment-service
```

---

## ACL Operations Reference

 Operation  Applies To  Description
---------
 Read  Topic, Group  Consume from topic / use consumer group
 Write  Topic  Produce to topic
 Create  Topic, Cluster  Create topics
 Delete  Topic  Delete topics
 Alter  Topic, Cluster  Modify configurations
 Describe  Topic, Group  Read metadata (list, describe)
 AlterConfigs  Topic, Broker  Change configs
 ClusterAction  Cluster  Broker-to-broker operations

---

## Role-Based Access Control (RBAC)

Confluent Platform extends ACLs with RBAC:

Pre-defined roles:
- `SystemAdmin` — full cluster access
- `ClusterAdmin` — manage brokers, ACLs
- `Operator` — monitor and restart
- `ResourceOwner` — own specific topics/groups
- `DeveloperRead/DeveloperWrite/DeveloperManage` — topic-level

```bash
# Assign alice the DeveloperWrite role for orders topic
confluent iam rbac role-binding create \
  --principal User:alice \
  --role DeveloperWrite \
  --resource Topic:orders \
  --kafka-cluster-id <cluster-id>
```

---

## Encryption and Secrets Management

**Never put secrets in config files!**

Use:
- **Environment variables**: `${env:KAFKA_PASSWORD}`
- **HashiCorp Vault**: dynamic credentials with short TTL
- **AWS Secrets Manager**: auto-rotation for RDS/Kafka credentials
- **Kubernetes Secrets**: for containerized deployments

Kafka Config Provider pattern:
```properties
config.providers=vault
config.providers.vault.class=com.github.jcustenborder.kafka.config.vault.VaultConfigProvider
ssl.keystore.password=${vault:secret/kafka/ssl:keystore_password}
```

---

## Governance Best Practices

**Topic ownership:**
- Every topic has a declared owner (team or service)
- Owner is responsible for schema, retention, SLA
- Track in a service catalog or schema registry

**Naming conventions:**
```
<environment>.<domain>.<entity>.<event>
prod.payments.payment.completed
staging.orders.order.placed
dev.users.user.updated
```

**Retention governance:**
- Set explicit `retention.ms` — never use default (7 days may not match your SLA)
- Document retention rationale (compliance, operational, business)

---

## PII and Data Masking

Kafka is often a conduit for sensitive data:

**At-source masking** — mask before producing:
```java
order.setCardNumber(maskPan(order.getCardNumber()));  // "4111 **** **** 1111"
```

**Field-level encryption** — encrypt sensitive fields in the event:
```java
ProtectedField cardNumber = encrypt(order.getCardNumber(), customerKey);
```

**Kafka Streams masking** — mask in a pipeline before sinking to accessible topics:
```java
KStream<String, Order> masked = orders.mapValues(o -> {
    o.setCardNumber(mask(o.getCardNumber()));
    return o;
});
masked.to("orders-masked");
```

---

## Compliance Considerations

 Regulation  Kafka Impact
---------
 GDPR  Right to erasure → use compaction with tombstones or field-level encryption
 PCI DSS  Encrypt cardholder data in transit and at rest; restrict access via ACLs
 HIPAA  PHI must be encrypted; access control; audit logging
 SOC 2  Audit trails, access control, monitoring
 FedRAMP  Specific TLS versions, FIPS-approved algorithms

---

## Security Posture for Enterprise Deployments

Checklist:
- [ ] TLS 1.2+ on all listeners
- [ ] Authentication (SCRAM or OAuth) for all clients
- [ ] ACLs or RBAC for all topics
- [ ] No wildcard `*` ACLs in production
- [ ] Secrets in a vault (not config files)
- [ ] Audit logging enabled
- [ ] Schema Registry with schema validation
- [ ] PII fields masked or encrypted
- [ ] Network segmentation (Kafka not directly accessible from internet)
- [ ] Regular credential rotation

---

## Module 7 Summary

- Kafka has no security by default — all layers must be explicitly configured
- TLS encrypts data in transit; configure at broker and all clients
- SCRAM-SHA-512 is the practical choice for most deployments; OAuth for cloud-native
- ACLs grant fine-grained operation-level access per principal per resource
- RBAC (Confluent) simplifies access management with pre-defined roles
- Never store secrets in config files — use Vault, AWS Secrets Manager, or K8s Secrets
- Governance: naming conventions, ownership, retention policies
- PII: mask at source or encrypt fields before producing

---

## What's Next

**Module 8 — Observability & Operations**

- Core Kafka metrics (broker, producer, consumer)
- Monitoring with Prometheus, Grafana, Burrow
- Troubleshooting: under-replicated partitions, lag spikes, disk saturation

---

## Lab Preview — Lab 7

**Configure ACLs and Validate Access Control**

You will:
1. Enable SASL/SCRAM authentication on a Kafka cluster
2. Create users for producer and consumer services
3. Configure ACLs granting least-privilege access
4. Verify that unauthorized operations are rejected
5. Test TLS encryption with certificate verification

Environment: Docker Compose Kafka with security enabled
Time: 60 minutes

---

