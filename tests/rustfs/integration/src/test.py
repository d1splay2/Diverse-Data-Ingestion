from src.main import Boto3
import os, inspect, subprocess

# Overall test metrics
current_test_pos = 0
amount_of_tests = 0

# Tests specific metrics
bucket = 'landing'

class Result:
	def __init__(self, result_status, result_text, expected):
		self.result_status = result_status
		self.result_text = result_text
		self.expected = expected

def test(func):
	def wrapper():
		global current_test_pos, amount_of_tests
		res = func()
		assert res.result_status == res.expected, f'✕({current_test_pos}/{amount_of_tests}) {func.__name__} failed.\n' \
												  f'Error: {res.result_text}'
		current_test_pos += 1
		print(f'✓ ({current_test_pos}/{amount_of_tests}) {func.__name__} passed.')
	wrapper.__name__ = func.__name__
	wrapper._is_test = True
	return wrapper

def find_all_tests():
	return [
		obj
		for name, obj in inspect.getmembers(
			__import__(__name__),
		)
		if inspect.isfunction(obj) and getattr(obj, '_is_test', False)
	]

@test
def create_bucket():
	client = Boto3(
		os.environ.get('RUSTFS_ACCESS_KEY'),
		os.environ.get('RUSTFS_SECRET_KEY')
	)

	create_bucket_uri = 'http://rustfs:9000'
	res = client.put_request(f'{create_bucket_uri}/{bucket}')
	return Result(res.status_code, res.content, 200)

if __name__ == '__main__':
	tests = find_all_tests()
	amount_of_tests = len(tests)

	for test in tests:
		test()