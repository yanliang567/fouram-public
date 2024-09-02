from pymilvus import ExceptionsMessage


class ServerExceptionsMessage(ExceptionsMessage):
    RateLimitError = "request is rejected by grpc RateLimiter middleware, please retry later"
    FlushTimeout = "wait for flush timeout"
