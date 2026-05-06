from src.main import Boto3
from src.transform_data import S3Config, Transformer, calculate_offset
import polars as pl
import os, sys, inspect


# Overall test metrics
current_test_pos = 0
amount_of_tests = 0
batch_size = 0

# Tests specific metrics
bucket = 'landing'

class Result:
    def __init__(self, output, result_text, expected):
        self.output = output
        self.result_text = result_text
        self.expected = expected

def test(func):
    global current_test_pos, amount_of_tests

    res = func()
    assert res.output == res.expected, f'✕({current_test_pos}/{amount_of_tests}) {func.__name__} failed.\n' \
                                           f'Error: {res.result_text}'
    current_test_pos += 1
    print(f'✓ ({current_test_pos}/{amount_of_tests}) {func.__name__} passed.')

def collect_ordered_functions():
    global amount_of_tests
    module = sys.modules[__name__]
    funcs_with_order = []

    for name, obj in vars(module).items():
        if not inspect.isfunction(obj):
            continue
        try:
            sig = inspect.signature(obj)
        except (ValueError, TypeError):
            continue
        if 'order' in sig.parameters:
            param = sig.parameters['order']
            if param.default is not inspect.Parameter.empty:
                default = param.default
                funcs_with_order.append((default, obj))

    funcs_with_order.sort(key=lambda x: x[0])
    amount_of_tests = len(funcs_with_order)
    return [func for _, func in funcs_with_order]

def create_bucket(order = 1):
    client = Boto3(
        os.environ.get('RUSTFS_ACCESS_KEY'),
        os.environ.get('RUSTFS_SECRET_KEY')
    )

    create_bucket_uri = 'http://rustfs:9000'
    res = client.put_request(f'{create_bucket_uri}/{bucket}')
    return Result(res.status_code, res.content, 200)

def write_delta(order = 2):
    global batch_size

    df = pl.read_csv('/opt/test-data/Reviews.csv')
    batch_size = int(len(df) / 9)

    tranfromer = Transformer(df, batch_size, S3Config('landing'))
    try:
        return Result(tranfromer.write_delta(), 'All good!', None)
    except Exception as e:
        return Result(e, e, None)

def delta_validation(order = 3):
    df = pl.read_csv('/opt/test-data/Reviews.csv')

    expected_df = df[calculate_offset(batch_size, 0)]

    s3_config = S3Config('landing')

    data_from_s3 = pl.read_delta(
        s3_config.create_s3_destionation('test'),
        storage_options=s3_config.credentials
    )

    result = expected_df.equals(data_from_s3)

    return Result(result, "Data written in S3 storage doesn't line up with actual data", True)

if __name__ == '__main__':
    ordered = collect_ordered_functions()
    for func in ordered:
        test(func)
