import os
from datetime import datetime

import falcon


class APIKeyMiddleware:
    def __init__(self):
        self.valid_api_keys = {os.getenv("API_KEY", "")}

    def process_request(self, req, resp):
        api_key = req.get_header("X-API-KEY")
        if api_key and api_key not in self.valid_api_keys:
            raise falcon.HTTPUnauthorized(
                title="Unauthorized", description="Invalid or missing API key."
            )


class ResponseTimeMiddleware:
    def process_request(self, req, resp):
        req.context.start_time = datetime.now()

    def process_response(self, req, resp, resource, req_succeeded):
        if not hasattr(req.context, "start_time"):
            return
        elapsed_time = str(datetime.now() - req.context.start_time)
        resp.set_header("X-Response-Time", elapsed_time)
