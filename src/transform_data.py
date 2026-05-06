import polars as pl
import os

class S3Config:
    def __init__(
        self,
        bucket: str,
    ):
        self.bucket = bucket
        self.create_s3_credentials()

    def create_s3_destionation(self, table_name):
        return f's3://{self.bucket}/{table_name}'

    def create_s3_credentials(self):
        self.credentials = {
            'aws_region': 'us-east-1',
            'aws_access_key_id': os.environ.get('RUSTFS_ACCESS_KEY'),
            'aws_secret_access_key': os.environ.get('RUSTFS_SECRET_KEY'),
            'allow_http': 'true',
            'force_path_style': 'true',
            'aws_endpoint_url': 'http://rustfs:9000',
        }

class Transformer:
    def __init__(
        self,
        df: pl.DataFrame,
        batch_size: int,
        s3_config: S3Config
    ):
        self.df = df
        self.batch_size = batch_size
        self.current_pos = 0
        self.s3_config = s3_config

    def write_delta(self):
        df_slice = self.helper()
        df_slice.write_delta(
            target=self.s3_config.create_s3_destionation('test'),
            mode='overwrite',
            storage_options=self.s3_config.credentials)

    def helper(self):
        result = self.df[calculate_offset(self.batch_size, self.current_pos)]
        self.current_pos += 1
        return result

def calculate_offset(batch_size: int, current_pos: int):
    start = batch_size * current_pos
    end = batch_size * (current_pos + 1)
    return slice(start, end)

def main():
    df = pl.read_csv('/opt/data/Reviews.csv')
    batch_size = int(len(df) / 9)

    tranfromer = Transformer(df, batch_size, S3Config('landing'))

    tranfromer.write_delta()

if __name__ == '__main__':
    main()
