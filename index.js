// Order Microservice Handler - Fast Asynchronous Checkout (Node.js)
const express = require('express');
const Redis = require('ioredis');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const redis = new Redis({ host: process.env.REDIS_HOST, port: 6379 });
const kafka = new Kafka({ clientId: 'order-service', brokers: [process.env.KAFKA_BROKER] });
const producer = kafka.producer();

// Inisialisasi Producer Kafka
async function initKafka() { 
    await producer.connect(); 
}
initKafka();

app.post('/api/v1/checkout', async (req, res) => {
    const { userId, productId, quantity } = req.body;
    const stockKey = `product:stock:${productId}`;

    try {
        // 1. Atomic Decrement Stok di Redis Cache (Atomic Locking, ~ 1-2 ms, Mencegah Overselling)
        const currentStock = await redis.decrby(stockKey, quantity);
        
        if (currentStock < 0) {
            // Rollback counter jika stok habis
            await redis.incrby(stockKey, quantity);
            return res.status(400).json({ 
                success: false, 
                message: "Stok Produk Habis (Flash Sale Finished)" 
            });
        }

        // 2. Publish Order Event ke Apache Kafka Topic (Asynchronous Buffering)
        const orderPayload = { 
            orderId: `ORD-${Date.now()}-${userId}`, 
            userId, 
            productId, 
            quantity, 
            timestamp: new Date() 
        };
        
        await producer.send({
            topic: 'flash-sale-order-created',
            messages: [{ key: String(productId), value: JSON.stringify(orderPayload) }]
        });

        // 3. Kembalikan Respon Cepat (HTTP 202 Accepted) tanpa menunggu DB Write
        return res.status(202).json({
            success: true,
            message: "Pesanan Berhasil Diterima & Sedang Diproses",
            orderId: orderPayload.orderId
        });
    } catch (err) {
        return res.status(500).json({ 
            success: false, 
            message: "Internal Server Error", 
            error: err.message 
        });
    }
});

app.listen(3000, () => console.log('Order Service running on port 3000'));
