import io
import sys
import requests
import zipfile
import pandas as pd

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

if __name__=="__main__":
    # a = cvm_builder("http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_2021.zip")
    # a.get_data()
    # print(a.df)

    # a = inf_diario("202112")
    # a.get_data()
    # print(a.list_files())
    # sys.exit()


    # a = inf_diario(2016)
    # a.get_data()
    # print(a.list_files())
    # dfs = []
    # for i in a.list_files():
    #     dfs.append(pd.read_csv(io.BytesIO(a.select_file(i)), sep=";", encoding="ansi"))
    #     print(len(dfs))
    # print(len(dfs))
    # df = pd.concat(dfs)
    # print(df.head())
    # print(df.shape)
    # print(df.info())
    # sys.exit()

    dfs = []
    for i in range(2005, 2021, 1):
        a = inf_diario(i)
        a.get_data()
        print(a.list_files())
        for i in a.list_files():
            dfs.append(pd.read_csv(io.BytesIO(a.select_file(i)), sep=";", encoding="ansi"))
        print(len(dfs))
    print(len(dfs))
    df = pd.concat(dfs)
    df = df.rename(columns=lambda x: x.lower())
    df = df.rename({"vl_patrim_liq": "vl_pl",
                    "captc_dia": "cpt",
                    "resg_dia": "rgt",
                    "nr_cotst": "n_cts",
                   })

    print(df.head())
    print(df.shape)
    print(df.info())
    df.to_csv("fi_doc_info_diario.csv", index=False)
