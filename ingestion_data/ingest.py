import pandas as pd

class Extraction():
    def __init__(self) -> None:
        self.path: str
        self.url: str
        self.data = pd.DataFrame()

    def static_file(self, path: str):
        self.path = path
        self.extension = self.__ext_checker()
        if self.extension == "csv":
            self.__read_csv()
        elif self.extension == "json":
            self.__read_json()
        elif self.extension == "parquet":
            self.__read_parquet()
        else:
            pass
        return self.data
        
    def __ext_checker(self) -> str:
        return self.path.split(".")[2]
    
    def __read_json(self):
        self.data = pd.read_json("./dataset/2017-10-02-1.json", lines=True)

        """
        lines = True 

        you attempt to import a JSON file into a pandas DataFrame, 
        yet the data is written in lines separated by endlines like '\n'.
        """

    def __read_parquet(self):
        self.data = pd.read_parquet("./dataset/yellow_tripdata_2023-01.parquet", engine="pyarrow")

        """
        Parquet library to use. If 'auto', then the option io.parquet.engine is used. 
        The default io.parquet.engine behavior is to try 'pyarrow’, falling back to 'fastparquet’ if 'pyarrow' is unavailable.

        When using the 'pyarrow' engine and no storage options are provided and a filesystem is implemented by both pyarrow.fs and fsspec (e.g. “s3://”), 
        then the pyarrow.fs filesystem is attempted first. 

        Use the filesystem keyword with an instantiated fsspec filesystem if you wish to use its implementation.

        https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html

        """


    def __read_csv(self) -> pd.DataFrame:
        """
        problem: DtypeWarning: Columns (6) have mixed types.Specify dtype option on import or set low_memory=False.
        to solve:

        dtype = {
            'first_name': str,
            'last_name': str,
            'date': str,
            'salary': str
        }

        df = pd.read_csv(
            'employees.csv',
            sep=',',
            encoding='utf-8',
            dtype=dtype

        )

        """
        self.data = pd.read_csv(self.path)

    def request_api(self, url):
        self.url = url

        # self.__requests_chunked()
        self.__read_json_chunked()

    def __requests_chunked(self) -> None:
        import requests
        from gzip import decompress
        from json import loads

        # resp = loads(decompress(requests.get(self.url)))
        # self.data = pd.DataFrame(resp)
        pass

    def __read_json_chunked(self) -> None:
        """Read github data from web with read_json to pandas DataFrame"""
        header = {'User-Agent': 'pandas'}
        chunk_size = 50000
        with pd.read_json(self.url, lines=True, storage_options=header, chunksize=chunk_size, compression="gzip") as reader: 
            for chunk in reader:
                self.data = self.data.append(chunk, ignore_index=True)
        print(self.data.head())

    def clean_data(self):
        import json

        """Fix dtype issues"""
        self.data["id"] = self.data["id"].astype("Int64")
        self.data["payload"] = self.data["payload"].apply(lambda x: json.dumps(x)).astype("string")
        self.data["type"] = self.data["type"].astype("string")
        self.data["created_at"] = pd.to_datetime(self.data["created_at"])

    def cast_data(self):
        pass

class Load():
    def __init__(self) -> None:
        pass
    
    def to_postgres():
        pass

def main():
    extract = Extraction()

    # read data from static file to dataframe
    # file_path = "./dataset/yellow_tripdata_2020-07.csv"
    # file_path = "./dataset/2017-10-02-1.json"
    # file_path = "./dataset/yellow_tripdata_2023-01.parquet"
    # result = extract.static_file(file_path)


    # read data from github dataset to dataframe (2 ways: with read_json and requests)
    year, month, day, hour = 2023, 10, 1, 1
    url = f"http://data.gharchive.org/{year}-{month:02}-{day:02}-{hour}.json.gz"
    print(">>>> url ", url)
    extract.request_api(url)

    # load = Load()
    # load.to_postgres()


if __name__ == "__main__":
    main()

# Loading data into a data frame 
# Investigating data in a data frame 
"""
- choose csv/json/parquet data
- investigate schema 
- convert appropriate data type
"""
# Casting data based on appropriate data types (timestamp, int, float/double, boolean, string/varchar, json object)

# Cleaning data, handle missing value, deal with NaN value 

# load data to a data warehouse (postgres) 

# Check data in data warehouse with DBeaver

# [TASK] Ingest data new york taxi to PostgreSQL