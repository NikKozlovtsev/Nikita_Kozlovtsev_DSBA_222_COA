import time
import uuid
from datetime import datetime
from concurrent import futures

import grpc
import posts_pb2
import posts_pb2_grpc

POSTS_DB = {}  # { id: {...}, ... }

class PostServiceServicer(posts_pb2_grpc.PostServiceServicer):
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