import pandas as _pd
from .base_producer import BaseProducer

class RangeTableFactory(BaseProducer):

    def __init__(
        self, 
        list_values:list[dict], 
    ) -> None:
        if not isinstance(list_values, list):
            raise(ValueError('O objeto deve ser uma `list[dict]'))
        self._list_values = list_values

    def extract(self):
        tbl = _pd.DataFrame(self._list_values)
        columns = {'no_coluna', 'inicio', 'fim', 'passo'}

        columns_tbl = set(tbl.columns)
        if len(columns.difference(columns_tbl)) == 0:
            return tbl
        else:
            raise(
                ValueError(
                    f'Nome de colunas inválidos!'
                    f'Especificar a lista de entrada da seguinte forma:'
                    f"""
                        [
                            {'no_coluna': 'a', 'inicio': 1, 'fim': 10, 'passo': 1},
                            {'no_coluna': 'b', 'inicio': 1, 'fim': 10, 'passo': 1},
                            {'no_coluna': 'c', 'inicio': 1, 'fim': 10, 'passo': 1}
                        ]
                    """
                )
            )

