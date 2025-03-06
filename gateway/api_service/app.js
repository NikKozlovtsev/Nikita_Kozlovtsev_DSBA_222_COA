const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');

const app = express();

// Проксируем все запросы, начинающиеся с /users, на User Service
app.use('/users', createProxyMiddleware({
  target: 'http://user_service:5000',
  changeOrigin: true,
}));

app.get('/', (req, res) => {
  res.send('API Gateway is running!');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API Gateway listening on port ${PORT}`);
});