# Order Service

Creates ecommerce orders, calculates totals, initiates non-COD payments, and tracks order state. It uses an outbox table so order changes can be published by Debezium and consumes payment outbox events to complete or cancel orders.

## Technology

- Java 17 and Spring Boot 3.5
- Spring Web, Security, and OAuth2 Resource Server
- Spring Data JPA with MySQL
- Kafka and Debezium outbox CDC
- OpenFeign
- MapStruct

## Local dependencies

| Dependency | Default address | Purpose |
| --- | --- | --- |
| MySQL | `localhost:3307/db_order` | Orders and `outbox_order` |
| Kafka | `localhost:9092` | Transaction test and CDC events |
| Kafka Connect | `http://localhost:8083` | Outbox publication |
| Keycloak | `http://localhost:8000/realms/dat-microservices` | JWT validation |
| Payment service | `http://localhost:8084` | Create non-COD payments |

The application listens on port `8084` and uses MySQL credentials `root`/`root` by default. The included compose file starts Zipkin only.

## Run locally

```powershell
docker compose up -d
mvn spring-boot:run
```

Run tests with:

```powershell
mvn test
```

## HTTP API

| Method | Path | Authentication | Description |
| --- | --- | --- | --- |
| `POST` | `/api/v1/orders/create?userId={id}` | Currently public | Create an order and optionally initiate payment |
| `GET` | `/api/v1/orders/test-transaction` | Currently public | Publish a test record that exercises retry/DLT handling |

Example order:

```json
{
  "paymentMethod": "COD",
  "productId": "product-123",
  "productName": "Keyboard",
  "productPrice": 79.99,
  "productQuantity": 2,
  "bankingMethod": null
}
```

```powershell
curl.exe -X POST "http://localhost:8084/api/v1/orders/create?userId=user-123" `
  -H "Content-Type: application/json" `
  -d '{"paymentMethod":"COD","productId":"product-123","productName":"Keyboard","productPrice":79.99,"productQuantity":2,"bankingMethod":null}'
```

Use a payment method other than `COD` to enter `WAITING_BANKING` and call payment service.

## Messaging

| Direction | Topic | Purpose |
| --- | --- | --- |
| Publishes | `test-order-topic` | Transaction/retry test |
| Consumes | `test-order-topic` | Deliberately fails to exercise dead-letter handling |
| Consumes | `test-order-topic-dlt` | Logs dead-letter records |
| Consumes | `outbox.db_payment.outbox_payment` | Update order state from payment status |
| Publishes through CDC | `outbox.db_order.outbox_order` | Notify inventory and other consumers of order state |

Register the outbox connector after Kafka Connect is healthy:

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  --data-binary "@outbox_order_connector.json"
```

## Integration note

The payment Feign client is currently hard-coded to `http://localhost:8084`, which is also this service's own port. Run payment on a distinct port and update the Feign client URL before testing non-COD orders.
