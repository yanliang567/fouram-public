

class ServerExceptionsMessage:
    RateLimitError = "request is rejected by grpc RateLimiter middleware, please retry later"
