from abc import ABC, abstractclassmethod

class BaseProducer(ABC):
    
    @abstractclassmethod
    def extract(self):...