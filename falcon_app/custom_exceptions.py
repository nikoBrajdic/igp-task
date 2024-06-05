import falcon


class EmptyRequestBody(falcon.HTTPBadRequest):
    def __init__(self):
        super().__init__(title="Empty request body.")


class EmptyPayload(falcon.HTTPBadRequest):
    def __init__(self):
        super().__init__(title="Submitted payload empty.")


class NoNumbersProvided(falcon.HTTPBadRequest):
    def __init__(self):
        super().__init__(title="No numbers provided.")


class InvalidJSON(falcon.HTTPBadRequest):
    def __init__(self, title="Invalid JSON string payload."):
        super().__init__(title=title)


class MissingKey(falcon.HTTPBadRequest):
    def __init__(self, key):
        super().__init__(
            title="Missing key.", description="JSON must contain key {}".format(key)
        )


class NonNumberProvided(falcon.HTTPBadRequest):
    def __init__(self):
        super().__init__(
            title="Non-numbers provided.",
            description='"numbers" must contain only numbers',
        )


class MessageNotSent(Exception):
    def __init__(self):
        super().__init__(f"Message failed to send.")


class MessageInProcess(Exception):
    def __init__(self, offset):
        super().__init__(f"message #{offset} being processed, retrying.")
