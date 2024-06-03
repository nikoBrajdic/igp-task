import falcon

from middleware import APIKeyMiddleware, ResponseTimeMiddleware
from resources import TakeNumbers, TakeFinalNumbers


app = falcon.App(
    middleware=[
        APIKeyMiddleware(),
        ResponseTimeMiddleware(),
    ]
)
app.add_route("/numbers", TakeNumbers())
app.add_route("/final_numbers", TakeFinalNumbers())
