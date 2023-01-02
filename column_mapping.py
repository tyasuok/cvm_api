# inf_diario
# df = df.rename(columns=lambda x: x.lower()).rename(columns=inf_diario_cols)
inf_diario_cols = {"tp_fundo": "tipo",
                   "dt_comptc": "data",
                   "vl_quota": "vl_cota",
                   "vl_patrim_liq": "pl",
                   "captc_dia": "captacao",
                   "resg_dia": "resgate"
}
inf_diario_cols_order = ["data", "cnpj_fundo", "vl_cota",
                         "pl", "captacao", "resgate",
                         "tipo", "nr_cotst", "vl_total",
]

cad_fnd_cols = {"tp_fundo": "tipo",
                "denom_social": "nome_fundo",
                "fundo_cotas": "cotas",
                "fundo_exclusivo": "exclusivo",
}
cad_fnd_cols_order = ["cnpj_fundo", "tipo", "nome",
                      "dt_reg", "dt_const", "cd_cvm",
                      "dt_cancel", "sit", "dt_ini_sit",
                      "dt_ini_ativ", "dt_ini_exerc", "dt_fim_exerc",
                      "classe", "dt_ini_classe", "rentab_fundo",
                      "condom", "cotas", "exclusivo",
                      "trib_lprazo", "publico_alvo", "entid_invest",
                      "taxa_perfm", "inf_taxa_perfm", "taxa_adm",
                      "inf_taxa_adm", "diretor", "cnpj_admin",
                      "admin", "pf_pj_gestor", "cpf_cnpj_gestor",
                      "gestor", "cnpj_auditor", "auditor",
                      "cnpj_custodiante", "custodiante", "cnpj_controlador",
                      "controlador",
]
