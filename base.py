import io
import abc
import zipfile
import psycopg2
import requests
import pandas as pd
from column_mapping import *

# pretty sure this isn't used anywhere besides historical
class cvm_dados:
    def __init__(self, link):
        self.link = link

    def get_data(self):
        self.data = requests.get(self.link)

    def list_files(self):
        return zipfile.ZipFile(io.BytesIO(self.data.content)).namelist()

    def select_file(self, file_name):
        """
        Only if file is a zip
        """
        self.zip = zipfile.ZipFile(io.BytesIO(self.data.content))
        return self.zip.read(file_name)

class get_data(abc.ABC):
    def __init__(self, url):
        self.url = url
        
    @abc.abstractmethod
    def download(self):
        pass

    @abc.abstractmethod
    def make_df(self):
        pass

class zip_data(get_data):
    def download(self):
        self.data = requests.get(self.url)
        _zip = zipfile.ZipFile(io.BytesIO(self.data.content))
        if len(_zip.namelist()) > 1:
            self.data = {i: io.BytesIO(_zip.read(i)) for i in _zip.namelist()}
        else:
            self.data = io.BytesIO(_zip.read(_zip.namelist()[0]))

    def make_df(self):
        self.download()
        if type(self.data) == dict:
            self.df = {i: pd.read_csv(j, sep=";", encoding="ansi") for i, j in self.data.items()}
        else:
            self.download()
            self.df = pd.read_csv(self.data, sep=";", encoding="ansi")

        return self

class csv_data(get_data):
    def download(self):
        self.data = io.BytesIO(requests.get(self.url).content)
    def make_df(self):
        self.download()
        self.df = pd.read_csv(self.data, sep=";", encoding="ansi")
        return self

class data_factory():
    def __new__(cls, fmt):
        if fmt == "ZIP":
            return zip_data
        elif fmt == "CSV":
            return csv_data
