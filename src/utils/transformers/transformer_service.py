import pandas as _pd

from .base_transformer import BaseTransformer as _BaseTransformer

class TransformerService():

    def __init__(self, transformers:list[ _BaseTransformer]=None) -> None:
        self._transformers = []
        if transformers:
            self.subscribe_transformers(transformers)

    def subscribe_transformer(self, transformer: _BaseTransformer):
        if not transformer in self._transformers:
            self._transformers.append(transformer)
        else:
            raise ValueError(f"Falha ao inserir. Transformer: {transformer} já existe")
        
    def subscribe_transformers(self, transformers: list[_BaseTransformer]):
        for t in transformers:
            self.subscribe_transformer(t)

    def handle_request(self, dataframe:_pd.DataFrame, check_request: bool=True):
        if check_request:
            for t in self._transformers:
                if t.check_request(dataframe):
                    dataframe = t.handle_request(dataframe)
        else:
            for t in self._transformers:
                dataframe = t.handle_request(dataframe)

        return dataframe
                