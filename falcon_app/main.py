import falcon

from middleware import APIKeyMiddleware, ResponseTimeMiddleware
from resources import ProduceNumbers, ConsumeFinalNumbers


app = falcon.App(
    middleware=[
        APIKeyMiddleware(),
        ResponseTimeMiddleware(),
    ]
)
app.add_route("/numbers", ProduceNumbers())
app.add_route("/final_numbers", ConsumeFinalNumbers())
