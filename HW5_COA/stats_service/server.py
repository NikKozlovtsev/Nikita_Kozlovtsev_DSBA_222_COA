import threading
import json
import time
import grpc
from concurrent import futures
from clickhouse_driver import Client
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import stats_pb2
import stats_pb2_grpc

def make_consumer(topics):
    """Пробуем подключиться к Kafka; если брокер недоступен, ждём и повторяем до 30 раз."""
    retries = 0
    while True:
        try:
            return KafkaConsumer(
                *topics,
                bootstrap_servers=['kafka:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
        except NoBrokersAvailable:
            retries += 1
            if retries > 30:
                raise RuntimeError("Kafka broker never came up for consumer")
            print(f"[stats_service] Kafka not ready for consumer, retry #{retries}, waiting 2s…")
            time.sleep(2)

def consume_events(ch_client):
    """
    Фоновые чтение из Kafka (топики post-views, post-likes, post-comments)
    и вставка в ClickHouse.
    """
    consumer = make_consumer(['post-views', 'post-likes', 'post-comments'])
    topic_event_map = {
        'post-views':    'view',
        'post-likes':    'like',
        'post-comments': 'comment'
    }

    for msg in consumer:
        ev_name = topic_event_map[msg.topic]
        data = msg.value
        ch_client.execute(
            """
            INSERT INTO default.events (user_id, entity_id, event, timestamp)
            VALUES
            """,
            [(data['clientId'], data['postId'], ev_name, data['timestamp'])]
        )

class StatsServicer(stats_pb2_grpc.StatsServiceServicer):
    def __init__(self, ch_client: Client):
        self.ch = ch_client

    def GetTotals(self, req, ctx):
        q = """
           SELECT
             sumIf(1, event='view'    AND entity_id=%(post)s) AS views,
             sumIf(1, event='like'    AND entity_id=%(post)s) AS likes,
             sumIf(1, event='comment' AND entity_id=%(post)s) AS comments
           FROM default.events
        """
        views, likes, comments = self.ch.execute(q, {'post': req.postId})[0]
        return stats_pb2.Totals(views=views, likes=likes, comments=comments)

    def _time_series(self, req, event_name):
        q = """
          SELECT toDate(timestamp) AS day, count() AS cnt
          FROM default.events
          WHERE event = %(ev)s AND entity_id = %(post)s
          GROUP BY day
          ORDER BY day
        """
        rows = self.ch.execute(q, {'ev': event_name, 'post': req.postId})
        return stats_pb2.TimeSeries(
            data=[stats_pb2.DateCount(day=str(day), count=cnt) for day, cnt in rows]
        )

    def GetViewsTimeSeries(self, req, ctx):
        return self._time_series(req, 'view')

    def GetLikesTimeSeries(self, req, ctx):
        return self._time_series(req, 'like')

    def GetCommentsTimeSeries(self, req, ctx):
        return self._time_series(req, 'comment')

    def GetTopPosts(self, req, ctx):
        mapping = {'VIEWS': 'view', 'LIKES': 'like', 'COMMENTS': 'comment'}
        ev = mapping.get(req.metric.name, 'view')
        q = f"""
          SELECT entity_id
          FROM default.events
          WHERE event = '{ev}'
          GROUP BY entity_id
          ORDER BY count() DESC
          LIMIT 10
        """
        rows = self.ch.execute(q)
        return stats_pb2.TopPosts(
            postIds=[stats_pb2.PostId(postId=r[0]) for r in rows]
        )

    def GetTopUsers(self, req, ctx):
        mapping = {'VIEWS': 'view', 'LIKES': 'like', 'COMMENTS': 'comment'}
        ev = mapping.get(req.metric.name, 'view')
        q = f"""
          SELECT user_id
          FROM default.events
          WHERE event = '{ev}'
          GROUP BY user_id
          ORDER BY count() DESC
          LIMIT 10
        """
        rows = self.ch.execute(q)
        return stats_pb2.TopUsers(
            userIds=[stats_pb2.UserId(userId=r[0]) for r in rows]
        )

def serve():
    # 1) Подключаемся к ClickHouse
    ch = Client(host='clickhouse', port=9000, database='default')

    # 2) Стартуем Kafka-консьюмер в фоне
    consumer_thread = threading.Thread(target=consume_events, args=(ch,), daemon=True)
    consumer_thread.start()

    # 3) Поднимаем gRPC-сервер
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    stats_pb2_grpc.add_StatsServiceServicer_to_server(StatsServicer(ch), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("StatsService is running on port 50052")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()