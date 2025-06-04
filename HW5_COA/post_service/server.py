import time
import uuid
from datetime import datetime
from concurrent import futures
from kafka import KafkaProducer
import json
import grpc
import posts_pb2
import posts_pb2_grpc

# at the top of your file, replace the straight import with:
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json


# remove any top‐level "producer = KafkaProducer(...)" and instead:

def make_producer():
    """Try until Kafka comes up."""
    retries = 0
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                value_serializer=lambda v: json.dumps(v).encode(),
            )
        except NoBrokersAvailable:
            retries += 1
            if retries > 30:
                raise RuntimeError("Kafka broker never came up")
            print("Kafka not ready, retrying in 2s…")
            time.sleep(2)

producer = make_producer()
POSTS_DB = {}  # { id: {...}, ... }


producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

COMMENTS_DB = {}  # храним комментарии в памяти
from posts_pb2 import GetDeletePostRequest, ViewLikeRequest, AddCommentRequest,ListCommentsRequest

def make_producer():
    retries = 0
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                value_serializer=lambda v: json.dumps(v).encode(),
            )
        except NoBrokersAvailable:
            retries += 1
            if retries > 30:
                raise RuntimeError("Kafka broker never came up")
            time.sleep(2)

producer = make_producer()

POSTS_DB    = {}
COMMENTS_DB = {}

class PostServiceServicer(posts_pb2_grpc.PostServiceServicer):
    # Просмотр
    def ViewPost(self, request: ViewLikeRequest, context):
        producer.send('post-views', {
            'postId': request.postId,
            'clientId': request.clientId,
            'timestamp': datetime.utcnow().isoformat()
        })
        # вызываем GetPost через правильный запрос
        return self.GetPost(
            GetDeletePostRequest(id=request.postId),
            context
        )

    # Лайк
    def LikePost(self, request: ViewLikeRequest, context):
        producer.send('post-likes', {
            'postId': request.postId,
            'clientId': request.clientId,
            'timestamp': datetime.utcnow().isoformat()
        })
        return self.GetPost(
            GetDeletePostRequest(id=request.postId),
            context
        )

    def AddComment(self, request, context):
        comment_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        comment = {
            'id': comment_id,
            'postId': request.postId,
            'clientId': request.clientId,
            'content': request.content,
            'createdAt': now
        }
        COMMENTS_DB.setdefault(request.postId, []).append(comment)
        producer.send('post-comments', comment)
        return posts_pb2.CommentResponse(
            comment=self._dict_to_comment(comment)  # <- теперь метод есть
        )

    # Список комментариев
    def ListComments(self, request, context):
        all_comments = COMMENTS_DB.get(request.postId, [])
        total = len(all_comments)
        page = max(request.page, 1)
        size = max(request.pageSize, 1)
        start = (page - 1) * size
        slice_ = all_comments[start:start + size]
        return posts_pb2.ListCommentsResponse(
            comments=[self._dict_to_comment(c) for c in slice_],
            totalCount=total
        )

    # Вспомогательный метод для преобразования dict -> protobuf.Comment
    def _dict_to_comment(self, c):
        return posts_pb2.Comment(
            id=c["id"],
            postId=c["postId"],
            clientId=c["clientId"],
            content=c["content"],
            createdAt=c["createdAt"],
        )
    def CreatePost(self, request, context):
        post_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        data = {
            "id": post_id,
            "title": request.title,
            "description": request.description,
            "creatorId": request.creatorId,
            "isPrivate": request.isPrivate,
            "tags": list(request.tags),
            "createdAt": now,
            "updatedAt": now
        }
        POSTS_DB[post_id] = data
        return posts_pb2.PostResponse(post=self._dict_to_post(data))

    def GetPost(self, request, context):
        if request.id not in POSTS_DB:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Post not found")
            return posts_pb2.PostResponse()
        data = POSTS_DB[request.id]
        return posts_pb2.PostResponse(post=self._dict_to_post(data))

    def UpdatePost(self, request, context):
        # Тут по схеме creatorId считаем ID поста
        post_id = request.creatorId
        if post_id not in POSTS_DB:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Post not found")
            return posts_pb2.PostResponse()
        now = datetime.utcnow().isoformat()
        data = POSTS_DB[post_id]
        data["title"] = request.title
        data["description"] = request.description
        data["isPrivate"] = request.isPrivate
        data["tags"] = list(request.tags)
        data["updatedAt"] = now
        return posts_pb2.PostResponse(post=self._dict_to_post(data))

    def DeletePost(self, request, context):
        if request.id not in POSTS_DB:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Post not found")
            return posts_pb2.PostResponse()
        deleted = POSTS_DB.pop(request.id)
        return posts_pb2.PostResponse(post=self._dict_to_post(deleted))

    def ListPosts(self, request, context):
        page = request.page if request.page >=1 else 1
        page_size = request.pageSize if request.pageSize > 0 else 10
        all_posts = list(POSTS_DB.values())
        total_count = len(all_posts)
        start = (page - 1)*page_size
        end = start+page_size
        slice_ = all_posts[start:end]
        posts = [self._dict_to_post(d) for d in slice_]
        return posts_pb2.ListPostsResponse(posts=posts, totalCount=total_count)

    def _dict_to_post(self, d):
        return posts_pb2.Post(
            id=d["id"],
            title=d["title"],
            description=d["description"],
            creatorId=d["creatorId"],
            isPrivate=d["isPrivate"],
            tags=d["tags"],
            createdAt=d["createdAt"],
            updatedAt=d["updatedAt"]
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    posts_pb2_grpc.add_PostServiceServicer_to_server(PostServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("post_service gRPC running on port 50051...")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()