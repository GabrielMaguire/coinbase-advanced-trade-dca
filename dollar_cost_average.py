import hashlib
import hmac
import http.client
import json
import math
import os
import time
import uuid
from enum import Enum	    # StrEnum introduced in Python 3.11
from http import HTTPStatus # HTTPMethod introduced in Python 3.11
from typing import TypedDict

LOCAL_EXECUTION: bool = True

class Side(str, Enum):
	BUY = 'BUY'
	SELL = 'SELL'

class Pair(str, Enum):
	""" Trading pair

	String format:
	<base_name>-<quote_name>
	"""

	BTC_USD  = 'BTC-USD'
	BTC_USDC = 'BTC-USDC'
	ETH_USD  = 'ETH-USD'
	ETH_USDC = 'ETH-USDC'

class Resource(str, Enum):
	ACCOUNTS = 'accounts'
	ORDERS = 'orders'
	PRODUCTS = 'products'

class HTTPMethod(str, Enum):
	GET = 'GET'
	POST = 'POST'

class CoinbaseApiCredentials(TypedDict):
	key: str
	secret: str

class CoinbaseDCA():
	BASE_ENDPOINT = '/api/v3/brokerage/'
	PRICE_DELTA = 0.002

	def __init__(self, creds: CoinbaseApiCredentials) -> None:
		self.creds = creds

	def create_order(self, side: Side, pair: Pair, quote_size: float):
		""" Create a limit order

		Base/quote currency:
		- Ex. BTC-USD: BTC = base, USD = quote

		@param side Buy or sell
		@param pair Trading pair
		@param quote_size Total amount of purchase in quote currency
				  (Ex. For BTC-USD pair, quote_size is in USD)
		"""

		assert pair.value in self.list_products()

		product = self.get_product(pair)

		assert quote_size >= float(product['quote_min_size'])
		assert quote_size <= float(product['quote_max_size'])

		# Calculate the purchase price of the base currency (with respect to the quote currency)
		quote_increment = float(product['quote_increment'])
		quote_max_digits = self._get_currency_sigfigs(quote_increment)
		price = float(product['price'])
		price_factor = 1.0 + (-self.PRICE_DELTA if side == Side.BUY else self.PRICE_DELTA)
		price_adjusted = round(price * price_factor, quote_max_digits)

		# Calculate the amount of base currency to purchase
		base_increment = float(product['base_increment'])
		base_max_digits = self._get_currency_sigfigs(base_increment)
		base_size_rounded = round(quote_size / price_adjusted, base_max_digits)

		assert base_size_rounded >= float(product['base_min_size'])
		assert base_size_rounded <= float(product['base_max_size'])

		method = HTTPMethod.POST
		resource = Resource.ORDERS
		body = json.dumps({
			"client_order_id": str(self._generate_client_order_id()),
			"side": side.value,
			"product_id": pair.value,
			"order_configuration": {
				"limit_limit_gtc": {
					"post_only": False,
					"limit_price": str(price_adjusted),
					"base_size": str(base_size_rounded)
				}
			}
		})
		return self._request(method, resource.value, body)

	def list_products(self) -> list[str]:
		response = self._request(HTTPMethod.GET, Resource.PRODUCTS.value)
		return [product['product_id'] for product in response['products']]

	def get_product(self, pair: Pair) -> dict:
		method = HTTPMethod.GET
		resource = '/'.join([Resource.PRODUCTS, pair])
		return self._request(method, resource)

	def _request(self, method: HTTPMethod, resource: str, body: str = ''):
		conn = http.client.HTTPSConnection('api.coinbase.com')
		timestamp = str(int(time.time()))
		endpoint = self.BASE_ENDPOINT + resource
		message = timestamp + method.value + endpoint + body
		signature = hmac.new(
			self.creds['secret'].encode('utf-8'),
			message.encode('utf-8'),
			digestmod=hashlib.sha256
		).hexdigest()

		headers = {
			"accept": "application/json",
			"CB-ACCESS-KEY": self.creds['key'],
			"CB-ACCESS-SIGN": signature,
			"CB-ACCESS-TIMESTAMP": timestamp
		}

		conn.request(method.value, endpoint, body, headers)
		res = conn.getresponse()
		data = res.read()

		if res.status == HTTPStatus.UNAUTHORIZED:
			print("Error: Unauthorized. Please check your API key and secret.")
			return None

		return json.loads(data)

	def _get_currency_sigfigs(self, currency_increment: float) -> int:
		assert currency_increment > 0.0
		return -int(math.floor(math.log10(currency_increment)))

	def _generate_client_order_id(self):
		return uuid.uuid4()

def load_api_credentials() -> CoinbaseApiCredentials:
	if LOCAL_EXECUTION:
		api_key = open('api_key', 'r').read().splitlines()[0]
		api_secret = open('api_secret', 'r').read().splitlines()[0]
	else:
		api_key = os.environ['API_KEY']
		api_secret = os.environ['API_SECRET']
	return {'key': api_key, 'secret': api_secret}

def execute_dca_buy():
	creds = load_api_credentials()
	coinbaseDCA = CoinbaseDCA(creds)

	if LOCAL_EXECUTION:
		trade_amount = 10.0
	else:
		trade_amount = float(os.environ['BTC_USDC_AMOUNT']) # Quantity in USD


	response = coinbaseDCA.create_order(Side.BUY, Pair.BTC_USDC, trade_amount)
	print(response)

def lambda_handler(event, context):
	execute_dca_buy()

if __name__ == "__main__":
	execute_dca_buy()
