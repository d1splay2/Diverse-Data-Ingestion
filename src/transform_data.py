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
            'aws_access_key_id': 'rustfsadmin',
            'aws_secret_access_key': 'rustfsadmin',
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
        df_copy = self.helper()
        df_copy.write_delta(
            target=self.s3_config.create_s3_destionation('test'),
            mode='overwrite',
            storage_options=self.s3_config.credentials)

    def calculate_offset(self):
        start = self.batch_size * self.current_pos
        self.current_pos += 1
        end = self.batch_size * self.current_pos
        return slice(start, end)

    def helper(self):
        return self.df[self.calculate_offset()]

def main():
    df = pl.read_csv('/opt/data/Reviews.csv')
    batch_size = int(len(df) / 9)

    tranfromer = Transformer(df, batch_size, S3Config('landing'))

    tranfromer.write_delta()

if __name__ == '__main__':
    main()
