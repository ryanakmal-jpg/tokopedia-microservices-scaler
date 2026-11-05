# Tokopedia Microservices Scalability Engine

Dokumentasi Perancangan Arsitektur Platform & Load Testing untuk Tugas Project Mata Kuliah **Teknologi Platform (FS215IP)** Universitas Raharja.

**Disusun Oleh:** Ryan Akmal Firdaus (NIM: 2422410904)

## 📌 Spesifikasi Target
- **Concurrency**: 100.000 RPS
- **Latensi Respon**: ≤ 100 ms (P95 Latency)
- **Ketersediaan**: 99.99% SLA
- **Strategi Stok**: Redis Atomic Locking (Bebas Overselling)
- **Buffering**: Apache Kafka Event Bus

## 🏗️ Topologi Arsitektur
Client (App/Web) -> API Gateway -> Order Microservice -> Redis Cluster & Apache Kafka -> PostgreSQL

## 📄 Purwarupa Kode (Order Service)
```javascript
// Quick Order Handler with Redis Atomic Decrement
app.post('/api/v1/checkout', async (req, res) => {
    const currentStock = await redis.decrby(`product:stock:${productId}`, quantity);
    if (currentStock < 0) return res.status(400).json({ message: "Stok Habis" });
    await producer.send({ topic: 'flash-sale-order-created', messages: [...] });
    return res.status(202).json({ message: "Pesanan Diproses" });
});
