import http.client
import hmac
import hashlib
import json
import time
import base64
import uuid
from enum import Enum
import math


class Side(Enum):
    BUY = 1
    SELL = 0


class Method(Enum):
    POST = 1
    GET = 0

def load_api_credentials():
    api_key = ''
    api_secret = ''
    return [api_key, api_secret]


creds = load_api_credentials()


def generate_client_order_id():
    return uuid.uuid4()


def coinbase_request(method, path, body):
    conn = http.client.HTTPSConnection("api.coinbase.com")
    timestamp = str(int(time.time()))
    message = timestamp + method + path.split('?')[0] + str(body)
    signature = hmac.new(creds[1].encode('utf-8'), message.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()

    headers = {
        "accept": "application/json",
        "CB-ACCESS-KEY": creds[0],
        "CB-ACCESS-SIGN": signature,
        "CB-ACCESS-TIMESTAMP": timestamp
    }

    conn.request(method, path, body, headers)
    res = conn.getresponse()
    data = res.read()

    # Check for Unauthorized status code (401)
    if res.status == 401:
        print("Error: Unauthorized. Please check your API key and secret.")
        return None

    try:
        response_data = json.loads(data.decode("utf-8"))
        print(json.dumps(response_data, indent=2))
        return response_data
    except json.JSONDecodeError:
        print("Error: Unable to decode JSON response. Raw response data:", data)
        return None


def placeLimitOrder(side, pair, size, limit_price):
    method = Method.POST.name
    path = '/api/v3/brokerage/orders'
    payload = json.dumps({
        "client_order_id": str(generate_client_order_id()),
        "side": side,
        "product_id": pair,
        "order_configuration": {
            "limit_limit_gtc": {
                "post_only": False,
                "limit_price": limit_price,
                "base_size": size
            }
        }
    })

    coinbase_request(method, path, payload)


def getAllProductInfo():
    method = Method.GET.name
    path = '/api/v3/brokerage/products'
    payload = ''
    response = coinbase_request(method, path, payload)
    for product in response['products']:
        print(product['product_id'])


def getProductInfo(pair):
    method = Method.GET.name
    path = f'/api/v3/brokerage/products/{pair}'
    payload = ''
    response = coinbase_request(method, path, payload)
    
    if response is None:
        return None

    return {"price": response['price'],
            "quote_increment": response['quote_increment'],
            "base_increment": response['base_increment']}


def lambda_handler(event, context):
    my_side = Side.BUY.name
    my_trading_pair = "BTC-USD"
    usd_order_size = 10
    factor = .998 if my_side == Side.BUY.name else 1.002

    product_info = getProductInfo(my_trading_pair)
    
    if product_info is None:
        print("Error: Unable to fetch product information.")
        return

    quote_currency_price_increment = abs(round(math.log(float(product_info['quote_increment']), 10)))
    base_currency_price_increment = abs(round(math.log(float(product_info['base_increment']), 10)))

    my_limit_price = str(round(float(product_info['price']) * factor, quote_currency_price_increment))
    my_order_size = str(round(usd_order_size / float(my_limit_price), base_currency_price_increment))

    placeLimitOrder(my_side, my_trading_pair, my_order_size, my_limit_price)

    print(f'The spot price of {my_trading_pair} is ${product_info["price"]}')
