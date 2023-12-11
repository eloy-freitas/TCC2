import pandas as _pd

from .base_transformer import BaseTransformer as _BaseTransformer


class NumberRangeTransformers(_BaseTransformer):
    def __init__(
        self,
     ):
        super().__init__()


    def check_request(self, dataframe:_pd.DataFrame):
        return isinstance(dataframe, _pd.DataFrame)
    
    def handle_request(self, dataframe:_pd.DataFrame):
        tbl_result = _pd.DataFrame()

        for row in dataframe.values:
            no_coluna = row[0]
            inicio = row[1]
            fim = row[2]
            passo = row[3]

            tbl_result[no_coluna] = range(inicio, fim, passo)


        return tbl_result
    