import falcon

from middleware import APIKeyMiddleware, ResponseTimeMiddleware
from resources import ProduceNumbers, ConsumeFinalNumbers


app = falcon.App(
    middleware=[
        APIKeyMiddleware(),
        ResponseTimeMiddleware(),
    ]
)
service_in = ProduceNumbers()
service_out = ConsumeFinalNumbers()
app.add_route("/numbers", service_in)
app.add_route("/final_numbers", service_out)
