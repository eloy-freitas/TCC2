import magic
import glob as _glob
import re


class FileUtils:

    def __init__(self): ...   

    def get_xlsx_from_path(self, base_path:str=None,  pattern:str='*'):
        if not isinstance(pattern, str):
            raise ValueError("Pattern inválido")
        
        files = []
        paths = _glob.glob(f"{base_path}/{pattern}")
        
        for path in paths:
            if 'Excel' in magic.from_file(path):
                print(f"Excel encontrado em: {path}")
                files.append(f"{path}")
        
        return files
        