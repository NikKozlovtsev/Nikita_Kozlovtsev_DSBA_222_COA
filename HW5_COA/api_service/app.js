/**
 * app.js — Главный API сервис (REST + Node.js).
 *
 * 1) /users/*  -> проксируем на User Service (http://user_service:5001).
 * 2) /posts/*  -> проверяем JWT, вызываем gRPC Post Service (post_service:50051).
 * 3) /stats/*  -> вызываем gRPC Stats Service (stats_service:50052).
 */

const express       = require('express');
const axios         = require('axios');
const jwt           = require('jsonwebtoken');
const path          = require('path');
const protoLoader   = require('@grpc/proto-loader');
const grpc          = require('@grpc/grpc-js');
const { Kafka }     = require('kafkajs');

const app = express();
app.use(express.json());

// ——— Переменные окружения / дефолтные значения ———
const USER_SERVICE_URL   = process.env.USER_SERVICE_URL   || 'http://user_service:5001';
const JWT_SECRET         = process.env.JWT_SECRET         || 'super-secret-key';
const POST_SERVICE_HOST  = process.env.POST_SERVICE_HOST  || 'post_service:50051';
const STATS_SERVICE_HOST = process.env.STATS_SERVICE_HOST || 'stats_service:50052';
const KAFKA_BROKERS      = [ process.env.KAFKA_BROKER || 'kafka:9092' ];

// ——— Общие опции для proto-loader ———
const PROTO_OPTS = {
  keepCase:     true,
  longs:        String,
  enums:        String,
  defaults:     true,
  oneofs:       true
};

// ——— Подключаем PostService (gRPC) ———
const POSTS_PROTO_PATH = path.join(__dirname, 'posts.proto');
const postsDef         = protoLoader.loadSync(POSTS_PROTO_PATH, PROTO_OPTS);
const postsPkg         = grpc.loadPackageDefinition(postsDef).postservice;
const postClient       = new postsPkg.PostService(
  POST_SERVICE_HOST,
  grpc.credentials.createInsecure()
);

// ——— Подключаем StatsService (gRPC) ———
const STATS_PROTO_PATH = path.join(__dirname, 'stats.proto');
const statsDef         = protoLoader.loadSync(STATS_PROTO_PATH, PROTO_OPTS);
const statsPkg         = grpc.loadPackageDefinition(statsDef).stats;
const statsClient      = new statsPkg.StatsService(
  STATS_SERVICE_HOST,
  grpc.credentials.createInsecure()
);

// ——— Kafka-продюсер ———
const kafka    = new Kafka({ brokers: KAFKA_BROKERS });
const producer = kafka.producer();
(async () => { await producer.connect(); })();

// ——— JWT-middleware ———
function verifyToken(req, res, next) {
  const auth = req.headers['authorization'];
  if (!auth) return res.status(401).json({ error: 'Missing Authorization header' });
  const token = auth.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Invalid token format' });
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.userId = decoded.sub;
    next();
  } catch {
    res.status(401).json({ error: 'Invalid or expired token' });
  }
}

// ——— gRPC-ошибки в HTTP ———
function handleGrpcError(err, res) {
  switch (err.code) {
    case grpc.status.NOT_FOUND:          return res.status(404).json({ error: 'Not Found' });
    case grpc.status.INVALID_ARGUMENT:   return res.status(400).json({ error: 'Invalid argument' });
    default:                             return res.status(500).json({ error: err.details });
  }
}

// ======= User Service (по HTTP-прокси) =======
app.post('/users/register', async (req, res) => {
  try {
    const r = await axios.post(`${USER_SERVICE_URL}/users/register`, req.body, {
      headers: { 'Content-Type': 'application/json' }
    });
    res.status(r.status).json(r.data);
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    res.status(500).json({ error: err.message });
  }
});

app.post('/users/login', async (req, res) => {
  try {
    const r = await axios.post(`${USER_SERVICE_URL}/users/login`, req.body, {
      headers: { 'Content-Type': 'application/json' }
    });
    res.status(r.status).json(r.data);
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    res.status(500).json({ error: err.message });
  }
});

app.get('/users/profile', verifyToken, async (req, res) => {
  try {
    const r = await axios.get(`${USER_SERVICE_URL}/users/profile`, {
      params: { user_id: req.userId },
      headers: { Authorization: req.headers.authorization }
    });
    res.status(r.status).json(r.data);
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    res.status(500).json({ error: err.message });
  }
});

app.put('/users/profile', verifyToken, async (req, res) => {
  try {
    const r = await axios.put(`${USER_SERVICE_URL}/users/profile`, req.body, {
      params: { user_id: req.userId },
      headers: {
        Authorization: req.headers.authorization,
        'Content-Type': 'application/json'
      }
    });
    res.status(r.status).json(r.data);
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    res.status(500).json({ error: err.message });
  }
});

// ======= Post Service (gRPC) =======
app.post('/posts',       verifyToken, (req, res) => {
  const data = {
    title:       req.body.title,
    description: req.body.description,
    creatorId:   req.userId,
    isPrivate:   req.body.isPrivate || false,
    tags:        req.body.tags || []
  };
  postClient.CreatePost(data, (e, r) => e ? handleGrpcError(e, res) : res.status(201).json(r.post));
});
app.get('/posts/:id',     verifyToken, (req, res) => {
  postClient.GetPost({ id: req.params.id }, (e, r) => e ? handleGrpcError(e, res) : res.json(r.post));
});
app.put('/posts/:id',     verifyToken, (req, res) => {
  const data = {
    title:       req.body.title,
    description: req.body.description,
    creatorId:   req.params.id,      // по .proto «костыль»
    isPrivate:   req.body.isPrivate || false,
    tags:        req.body.tags || []
  };
  postClient.UpdatePost(data, (e, r) => e ? handleGrpcError(e, res) : res.json(r.post));
});
app.delete('/posts/:id',  verifyToken, (req, res) => {
  postClient.DeletePost({ id: req.params.id }, (e, r) => e ? handleGrpcError(e, res) : res.json(r.post));
});
app.get('/posts',         verifyToken, (req, res) => {
  const page     = +req.query.page     || 1;
  const pageSize = +req.query.pageSize || 10;
  postClient.ListPosts({ page, pageSize }, (e, r) => e ? handleGrpcError(e, res) : res.json({
    posts:      r.posts,
    totalCount: r.totalCount
  }));
});

// ——— отправка в Kafka и вызов gRPC в одном ———
async function sendToKafka(topic, msg, res) {
  try {
    await producer.send({ topic, messages: [{ value: JSON.stringify(msg) }] });
  } catch (e) {
    return res.status(500).json({ error: `Kafka error: ${e.message}` });
  }
}

app.post('/posts/:id/view',    verifyToken, async (req, res) => {
  const msg = { postId: req.params.id, clientId: req.userId, timestamp: new Date().toISOString() };
  await sendToKafka('post-views', msg, res);
  postClient.ViewPost(msg, (e, r) => e ? handleGrpcError(e, res) : res.json({ ...r.post, viewedAt: msg.timestamp }));
});

app.post('/posts/:id/like',    verifyToken, async (req, res) => {
  const msg = { postId: req.params.id, clientId: req.userId, timestamp: new Date().toISOString() };
  await sendToKafka('post-likes', msg, res);
  postClient.LikePost(msg, (e, r) => e ? handleGrpcError(e, res) : res.json({ ...r.post, likedAt: msg.timestamp }));
});

app.post('/posts/:id/comments',verifyToken, async (req, res) => {
  const msg = {
    postId:    req.params.id,
    clientId:  req.userId,
    content:   req.body.content,
    timestamp: new Date().toISOString()
  };
  await sendToKafka('post-comments', msg, res);
  postClient.AddComment(msg, (e, r) => e ? handleGrpcError(e, res) : res.status(201).json(r.comment));
});

app.get('/posts/:id/comments', verifyToken, (req, res) => {
  const page     = +req.query.page     || 1;
  const pageSize = +req.query.pageSize || 10;
  postClient.ListComments({ postId: req.params.id, page, pageSize }, (e, r) =>
    e ? handleGrpcError(e, res) : res.json({ comments: r.comments, totalCount: r.totalCount })
  );
});

// ======= Stats Service (gRPC proxy) =======
function grpcProxy(fn) {
  return (req, res) => fn(req, (e, r) => e ? handleGrpcError(e, res) : res.json(r));
}

app.get('/stats/posts/:id/totals', grpcProxy((req, cb) =>
  statsClient.GetTotals({ postId: req.params.id }, cb)
));

app.get('/stats/posts/:id/views', grpcProxy((req, cb) =>
  statsClient.GetViewsTimeSeries({ postId: req.params.id }, cb)
));
app.get('/stats/posts/:id/likes', grpcProxy((req, cb) =>
  statsClient.GetLikesTimeSeries({ postId: req.params.id }, cb)
));
app.get('/stats/posts/:id/comments', grpcProxy((req, cb) =>
  statsClient.GetCommentsTimeSeries({ postId: req.params.id }, cb)
));

app.get('/stats/top/posts', grpcProxy((req, cb) => {
  const key       = req.query.metric?.toUpperCase();
  const metricVal = statsPkg.TopRequest.Metric[key] ?? statsPkg.TopRequest.Metric.VIEWS;
  statsClient.GetTopPosts({ metric: metricVal }, cb);
}));
app.get('/stats/top/users', grpcProxy((req, cb) => {
  const key       = req.query.metric?.toUpperCase();
  const metricVal = statsPkg.TopRequest.Metric[key] ?? statsPkg.TopRequest.Metric.VIEWS;
  statsClient.GetTopUsers({ metric: metricVal }, cb);
}));

// Health-check
app.get('/', (req, res) => res.send('Main API up'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Main API on port ${PORT}`));