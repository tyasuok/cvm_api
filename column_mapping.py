# inf_diario
# df = df.rename(columns=lambda x: x.lower()).rename(columns=inf_diario_cols)
inf_diario_cols = {"tp_fundo": "tipo",
                   "dt_comptc": "data",
                   "vl_quota": "vl_cota",
                   "vl_patrim_liq": "pl",
                   "captc_dia": "captacao",
                   "resg_dia": "resgate"
}

cad_fnd_cols = {"tp_fundo": "tipo",
                "denom_social": "nome_fundo",
                "fundo_cotas": "cotas",
                "fundo_exclusivo": "exclusivo",
}
