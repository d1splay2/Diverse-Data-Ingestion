import boto3, requests, hashlib, json
from typing import Optional
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

class Boto3:
	def __init__(self, access_key: str, secret_key: str):
		self.credentials = boto3.Session(
							aws_secret_access_key=secret_key,
							aws_access_key_id=access_key
						   ).get_credentials()

	def put_request(
			self,
			url: str,
			data: Optional[str] = None,
			region: str = 'us-east-1'
		):
		host = url.split('//')[1].split('/')[0]
		if data != None:
			data = json.dumps(data)
			headers = {
				'Content-Type': 'application/json',
				'Host': host,
				'x-amz-content-sha256': hashlib.sha256(data.encode('utf-8')).hexdigest()
			}
			request = AWSRequest(method='PUT', url=url, data=data, headers=headers)
		else:
			headers = {
				'Content-Type': 'application/json',
				'Host': host,
				'x-amz-content-sha256': hashlib.sha256(b'').hexdigest()
			}
			request = AWSRequest(method='PUT', url=url, headers=headers)
		SigV4Auth(self.credentials, 's3', region).add_auth(request)

		response = requests.put(url, headers=dict(request.headers), data=data)
		return response

if __name__ == '__main__':
	client = Boto3(
		'test',
		'test'
	)

	# client = Boto3(
	# 	os.environ.get('RUSTFS_ACCESS_KEY'),
	# 	os.environ.get('RUSTFS_SECRET_KEY')
	# )

	create_bucket_uri = 'http://localhost:9000/landing'
	client.put_request(create_bucket_uri)
