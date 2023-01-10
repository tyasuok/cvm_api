import io
import abc
import zipfile
import psycopg2
import requests
import pandas as pd
from column_mapping import *

# this is also "out of place", cause it doesn't use any of the builders.
# It's like this because of the way historical data is set up
class inf_diario(cvm_dados):
    def __init__(self, ano):
        super().__init__(f"http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{ano}.zip")


class cvm_helper:
    """
    incomplete, the ideia is for it to be an alternative to actually going to the website
    types:
        package
        group
        tag
    """
    def __init__(self, list_type="package"):
        r = requests.get(f"http://dados.cvm.gov.br/api/3/action/{list_type}_list")
        if r.ok:
            r = r.json()
        else:
            raise Exception("Erro na request, verifique o nome do tipo de lista")
        if r["success"]:
            self.result = r["result"]
        else:
            raise Exception("Erro na request, verifique o nome do tipo de lista")

    def get_package(self, resource_name):
        pass

class cvm_pkg:
    def __init__(self, pkg):
        self.pkg = pkg
        self.url = f"http://dados.cvm.gov.br/api/3/action/package_show?id={pkg}"
    def show_resources(self):
        self.resources = requests.get(self.url).json()["result"]["resources"]
    def get_all_data(self, concat=False):
        self.dfs = []
        if not hasattr(self, 'resources'):
            self.show_resources()
        for i in self.resources:
            print(i["name"], i["format"])
            data_getter = data_factory(i["format"])
            if data_getter:
                self.dfs.append(data_getter(i["url"]).make_df().df)
        if concat:
            self.df = pd.concat(self.dfs)

    def getone(self, resource):
        """
        Resource must have at least a `format` and an `url`
        """
        data_getter = data_factory(resource["format"])
        if data_getter:
            self.df = data_getter(resource["url"]).make_df().df

class bcb_sgs():
    def __init__(self, serie, ultimos=None, ini_fim=(None, None)):
        self.url = f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"
        if ultimos:
            self.url += f"/ultimos/{ultimos}"
            self.url += "?formato=json"
        elif all(ini_fim):
            self.url += "?formato=json"
            self.url += (
                         f"&dataInicial={ini_fim[0].strftime('%d/%m/%Y')}"
                         f"&dataFinal={ini_fim[1].strftime('%d/%m/%Y')}"
                        )
        else:
            self.url += "?formato=json"
    def get_data(self):
        r = requests.get(self.url)
        df = pd.DataFrame().from_records(r.json(), index="data")
        df = df.astype(float) / 100
        df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
        self.df = df

class db_handler():
    def __init__(self, dbname, user, password, cvm_pkg):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.cvm_pkg = cvm_pkg

    def _open_conn(self):
        self.conn = psycopg2.connect(dbname=self.dbname,
                                     user=self.user,
                                     password=self.password
        )
    def _gen_insert(self):
        """
        to-do: factory for different tables
        """
        gen_query = insert_factory(self.cvm_pkg)
        self.query = gen_query(self.cvm_pkg.df).gen().query

    def insert_data(self):
        self._open_conn()
        c = self.conn.cursor()
        c.execute(self.query)
        conn.commit()
        conn.close()

    def __del__(self):
        if self.conn:
            conn.close()

class insert_gen(abc.ABC):
    def __init__(self, df):
        self.df = df
        
    @abc.abstractmethod
    def gen(self):
        pass

class insert_inf_diario(insert_gen):
    def gen(self):
        tmp_values = (
                      "INSERT INTO "
                      "inf_diario (data, cnpj_fundo, vl_cota, "
                                  "pl, captacao, resgate, "
                                  "tipo, nr_cotst, vl_total) "
                      "VALUES "
        )
        for _, i in self.df.iterrows():
            tmp_values += (f"('{i['data']}', '{i['cnpj_fundo']}', {i['vl_cota']}, "
                           f"{i['pl']}, {i['captacao']}, {i['resgate']}, "
                           f"'{i['tipo']}', {i['nr_cotst']}, {i['vl_total']}), "
            )
        self.query = tmp_values[:-2] + ";"
        return self

class insert_cad_fnd:
    def gen(self):
        tmp_values = (
                      "INSERT INTO "
                      "inf_diario (cnpj_fundo, tipo, nome, "
                                   "dt_reg, dt_const, cd_cvm, "
                                   "dt_cancel, sit, dt_ini_sit, "
                                   "dt_ini_ativ, dt_ini_exerc, dt_fim_exerc, "
                                   "classe, dt_ini_classe, rentab_fundo, "
                                   "condom, cotas, exclusivo, "
                                   "trib_lprazo, publico_alvo, entid_invest, "
                                   "taxa_perfm, inf_taxa_perfm, taxa_adm, "
                                   "inf_taxa_adm, diretor, cnpj_admin, "
                                   "admin, pf_pj_gestor, cpf_cnpj_gestor, "
                                   "gestor, cnpj_auditor, auditor, "
                                   "cnpj_custodiante, custodiante, cnpj_controlador, "
                                   "controlador) "
                      "VALUES "
        )
        for _, i in self.df.iterrows():
            tmp_values += (
                           f"'({i['cnpj_fundo']}', '{i['tipo']}', '{i['nome']}', "
                           f"'{i['dt_reg']}', '{i['dt_const']}', {i['cd_cvm']}, "
                           f"'{i['dt_cancel']}', '{i['sit']}', '{i['dt_ini_sit']}', "
                           f"'{i['dt_ini_ativ']}', '{i['dt_ini_exerc']}', '{i['dt_fim_exerc']}', "
                           f"'{i['classe']}', '{i['dt_ini_classe']}', '{i['rentab_fundo']}', "
                           f"'{i['condom']}', '{i['cotas']}', '{i['exclusivo']}', "
                           f"'{i['trib_lprazo']}', '{i['publico_alvo']}', '{i['entid_invest']}', "
                           f"{i['taxa_perfm']}, '{i['inf_taxa_perfm']}', {i['taxa_adm']}, "
                           f"'{i['inf_taxa_adm']}', '{i['diretor']}', '{i['cnpj_admin']}', "
                           f"'{i['admin']}', '{i['pf_pj_gestor']}', '{i['cpf_cnpj_gestor']}', "
                           f"'{i['gestor']}', '{i['cnpj_auditor']}', '{i['auditor']}', "
                           f"'{i['cnpj_custodiante']}', '{i['custodiante']}', '{i['cnpj_controlador']}', "
                           f"'{i['controlador']}), '"
            )
        self.query = tmp_values[:-2] + ";"
        return self

class insert_factory:
    def __new__(cls, pkg):
        if pkg == "fi-doc-inf_diario":
            return insert_inf_diario
        elif pkg == "fi-cad":
            return insert_cad_fnd

if __name__=="__main__":
    # # pegar série histórica de fundos
    # dfs = []
    # for i in range(2005, 2021, 1):
    #     a = inf_diario(i)
    #     a.get_data()
    #     print(a.list_files())
    #     for i in a.list_files():
    #         dfs.append(pd.read_csv(io.BytesIO(a.select_file(i)), sep=";", encoding="ansi"))
    #     print(len(dfs))
    # print(len(dfs))
    # df = pd.concat(dfs)
    # df = df.rename(columns=lambda x: x.lower())
    # df = df.rename({"vl_patrim_liq": "vl_pl",
    #                 "captc_dia": "cpt",
    #                 "resg_dia": "rgt",
    #                 "nr_cotst": "n_cts",
    #                })
    # print(df.shape)
    # print(df.info())
    # print(cvm_helper().result)
    a = cvm_pkg("fi-doc-inf_diario")
    a.show_resources()
    a.get_all_data()
    df = pd.concat(a.dfs)
    df = df.rename(columns=lambda x: x.lower()).rename(columns=inf_diario_cols)
    df["data"] = df["data"].dt.strftime("%Y-%m-%d")

    # df = df.rename(columns=lambda x: x.lower())
    # df = df.rename({"vl_patrim_liq": "vl_pl",
    #                 "captc_dia": "cpt",
    #                 "resg_dia": "rgt",
    #                 "nr_cotst": "n_cts",
    # })
    print(df.shape)
    print(df.info())
    print(df.head(2))
    print(df.tail(2))

    # df = pd.read_csv("fi_doc_info_diario.csv", nrows=5)
    # print(df.shape)
    # print(df.info())
    # print(df.head(2))
    # print(df.tail(2))

# a = cvm_helper()
# a.result
#  
# histórico do fi-cad
    b = cvm_pkg("fi-cad")
    b.show_resources()
    b.resources
    for i in b.resources:
        print(i["name"])

# b.getone(b.resources[1])
# b.df.info()
# b.df = b.df.drop(["VL_PATRIM_LIQ", "DT_PATRIM_LIQ"], axis=1)
# b.df["CD_CVM"] = b.df["CD_CVM"].astype("Int64")
# b.df["CONDOM"] = df["CONDOM"].apply(lambda x: x[0] if type(x)==str else x)
# # os dois são 23 msm
# if b.df["SIT"].str.len().max() > 23:
#     print("bruH sit")
# if b.df["CLASSE"].str.len().max() > 23:
#     print("bruH classe")
# if b.df["RENTAB_FUNDO"].str.len().max() > 55:
#     print("bruh rentab_fundo")
# if b.df["PUBLICO_ALVO"].str.len().max() > 13:
#     print("bruh rentab_fundo")
# b.df = b.df.rename(columns=lambda x: x.lower()).rename(columns=cad_fnd_cols)
# b.df.info()
# 
# [14,17,18,20,22,24,27,37,38]
# # b.df.keys()
# # # se o zip tem txt dentro e n csv eu nunca verifico isso
# # # no get_all_data
