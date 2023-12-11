from abc import ABC, abstractmethod

class BaseTransformer(ABC):

    @abstractmethod
    def handle_request(self, **kwargs)->object:...

    @abstractmethod
    def check_request(self, **kwargs)->bool:...
    